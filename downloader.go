package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

type Downloader struct {
	Endpoint string
	Token    string
	Workers  int
	Parts    int
	Retries  int
	Timeout  time.Duration
	Verify   bool
	Out      io.Writer
	Client   *http.Client
}

type repoFile struct {
	Path   string `json:"Path"`
	Type   string `json:"Type"`
	SHA256 string `json:"Sha256"`
	Size   int64  `json:"Size"`
}

type listResponse struct {
	Code    int    `json:"Code"`
	Success bool   `json:"Success"`
	Message string `json:"Message"`
	Data    struct {
		Files []repoFile `json:"Files"`
	} `json:"Data"`
}

func (d *Downloader) Download(ctx context.Context, repo, revision, output string, includes, excludes []string) error {
	// Use one transport for the whole download. Besides connection reuse, this also
	// makes the global request limit below describe actual network concurrency.
	copy := *d
	if copy.Client == nil {
		copy.Client = copy.client()
	}
	d = &copy
	if err := validateRepo(repo); err != nil {
		return err
	}
	root, err := filepath.Abs(output)
	if err != nil {
		return err
	}
	files, err := d.list(ctx, repo, revision)
	if err != nil {
		return err
	}
	selected := files[:0]
	var total int64
	for _, f := range files {
		if f.Type == "tree" || f.Path == "" || !selectedBy(f.Path, includes, excludes) {
			continue
		}
		if _, err := safeTarget(root, f.Path); err != nil {
			return fmt.Errorf("服务端返回了不安全路径 %q: %w", f.Path, err)
		}
		selected = append(selected, f)
		total += f.Size
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Path < selected[j].Path })
	if len(selected) == 0 {
		return errors.New("没有匹配的模型文件")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	fmt.Fprintf(d.writer(), "模型 %s@%s：%d 个文件，%s -> %s\n", repo, revision, len(selected), humanBytes(total), root)
	progress := newDownloadProgress(d.writer(), total, len(selected))
	defer progress.finish()

	jobs := make(chan repoFile)
	slots := make(chan struct{}, d.Workers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures []error
	for range d.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range jobs {
				if err := d.downloadFile(ctx, repo, revision, root, f, slots, progress); err != nil {
					mu.Lock()
					failures = append(failures, fmt.Errorf("%s: %w", f.Path, err))
					progress.log("失败  %s: %v\n", f.Path, err)
					mu.Unlock()
				} else {
					mu.Lock()
					progress.log("完成  %s (%s)\n", f.Path, humanBytes(f.Size))
					mu.Unlock()
				}
			}
		}()
	}
	for _, f := range selected {
		select {
		case jobs <- f:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return ctx.Err()
		}
	}
	close(jobs)
	wg.Wait()
	if len(failures) > 0 {
		return fmt.Errorf("%d 个文件下载失败（可重新运行以断点续传）: %w", len(failures), errors.Join(failures...))
	}
	progress.finish()
	fmt.Fprintf(d.writer(), "下载完成：%s\n", root)
	return nil
}

func (d *Downloader) list(ctx context.Context, repo, revision string) ([]repoFile, error) {
	u := d.Endpoint + "/api/v1/models/" + escapeRepo(repo) + "/repo/files"
	q := url.Values{"Revision": {revision}, "Recursive": {"true"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u+"?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	d.headers(req)
	resp, err := d.client().Do(req)
	if err != nil {
		return nil, fmt.Errorf("获取文件列表: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, responseError("获取文件列表", resp)
	}
	var result listResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&result); err != nil {
		return nil, fmt.Errorf("解析文件列表: %w", err)
	}
	if result.Code != 0 && result.Code != 200 || result.Message != "" && !result.Success && result.Code != 200 {
		return nil, fmt.Errorf("ModelScope API: code=%d, %s", result.Code, result.Message)
	}
	return result.Data.Files, nil
}

func (d *Downloader) downloadFile(ctx context.Context, repo, revision, root string, f repoFile, slots chan struct{}, progress *downloadProgress) error {
	fileProgress := progress.file(f.Size)
	target, err := safeTarget(root, f.Path)
	if err != nil {
		return err
	}
	if ok, err := d.validExisting(target, f); err == nil && ok {
		fileProgress.set(f.Size)
		progress.fileDone()
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	part := target + ".part"
	var last error
	for attempt := 0; attempt <= d.Retries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(1<<min(attempt-1, 5)) * time.Second
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		var downloadErr error
		if d.Parts > 1 && f.Size >= 8<<20 {
			downloadErr = d.downloadParallel(ctx, repo, revision, part, f, slots, fileProgress, progress)
			if errors.Is(downloadErr, errRangeUnsupported) {
				_ = os.Remove(part)
				_ = os.Remove(part + ".meta")
				fileProgress.set(0)
				downloadErr = d.downloadAttemptWithSlots(ctx, repo, revision, part, f, slots, fileProgress, progress)
			}
		} else {
			downloadErr = d.downloadAttemptWithSlots(ctx, repo, revision, part, f, slots, fileProgress, progress)
		}
		if downloadErr != nil {
			last = downloadErr
			continue
		}
		if err := verifyFile(part, f, d.Verify); err != nil {
			last = err
			_ = os.Remove(part)
			_ = os.Remove(part + ".meta")
			fileProgress.set(0)
			continue
		}
		if err := os.Rename(part, target); err != nil {
			return err
		}
		_ = os.Remove(part + ".meta")
		fileProgress.set(f.Size)
		progress.fileDone()
		return nil
	}
	return last
}

func (d *Downloader) downloadAttempt(ctx context.Context, repo, revision, part string, f repoFile) error {
	p := newDownloadProgress(io.Discard, f.Size, 1)
	return d.downloadAttemptWithSlots(ctx, repo, revision, part, f, nil, p.file(f.Size), p)
}

func (d *Downloader) downloadAttemptWithSlots(ctx context.Context, repo, revision, part string, f repoFile, slots chan struct{}, fileProgress *fileProgress, progress *downloadProgress) error {
	if err := acquire(ctx, slots); err != nil {
		return err
	}
	defer release(slots)
	progress.connection(1)
	defer progress.connection(-1)
	var offset int64
	if st, err := os.Stat(part); err == nil {
		offset = st.Size()
	}
	state := newPartState(repo, revision, f, 1)
	meta := part + ".meta"
	if offset > 0 {
		if !loadMatchingPartState(meta, state, &state) {
			_ = os.Remove(part)
			offset = 0
		}
	}
	if f.Size > 0 && offset > f.Size {
		_ = os.Remove(part)
		offset = 0
	}
	fileProgress.set(offset)
	if err := writePartState(meta, state); err != nil {
		return fmt.Errorf("保存续传状态: %w", err)
	}
	u := d.Endpoint + "/api/v1/models/" + escapeRepo(repo) + "/repo"
	q := url.Values{"Revision": {revision}, "FilePath": {f.Path}}
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	timer := time.AfterFunc(d.Timeout, cancel)
	defer timer.Stop()
	req, err := http.NewRequestWithContext(attemptCtx, http.MethodGet, u+"?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	d.headers(req)
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
		if state.Validator != "" {
			req.Header.Set("If-Range", state.Validator)
		}
	}
	resp, err := d.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable && f.Size > 0 && offset == f.Size {
		return nil
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return responseError("下载", resp)
	}
	appendMode := offset > 0 && resp.StatusCode == http.StatusPartialContent && validContentRange(resp.Header.Get("Content-Range"), offset, f.Size)
	if offset > 0 && resp.StatusCode == http.StatusPartialContent && !appendMode {
		return fmt.Errorf("服务端返回了不匹配的 Content-Range: %q", resp.Header.Get("Content-Range"))
	}
	if validator := responseValidator(resp); validator != "" && validator != state.Validator {
		state.Validator = validator
		if err := writePartState(meta, state); err != nil {
			return fmt.Errorf("保存续传状态: %w", err)
		}
	}
	if !appendMode {
		fileProgress.set(0)
	}
	flags := os.O_CREATE | os.O_WRONLY
	if appendMode {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
		offset = 0
	}
	out, err := os.OpenFile(part, flags, 0o644)
	if err != nil {
		return err
	}
	reader := &activityReader{r: resp.Body, timer: timer, timeout: d.Timeout, onRead: fileProgress.read}
	_, copyErr := io.CopyBuffer(out, reader, make([]byte, 1<<20))
	syncErr := out.Sync()
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

var errRangeUnsupported = errors.New("服务器不支持分片下载")

type partState struct {
	Version   int    `json:"version"`
	Repo      string `json:"repo"`
	Revision  string `json:"revision"`
	Path      string `json:"path"`
	SHA256    string `json:"sha256,omitempty"`
	Size      int64  `json:"size"`
	Parts     int    `json:"parts"`
	Validator string `json:"validator,omitempty"`
	Done      []bool `json:"done,omitempty"`
}

func newPartState(repo, revision string, f repoFile, parts int) partState {
	return partState{Version: 1, Repo: repo, Revision: revision, Path: f.Path, SHA256: f.SHA256, Size: f.Size, Parts: parts, Done: make([]bool, parts)}
}

func loadMatchingPartState(path string, expected partState, dst *partState) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var saved partState
	if json.Unmarshal(data, &saved) != nil || saved.Version != expected.Version || saved.Repo != expected.Repo || saved.Revision != expected.Revision || saved.Path != expected.Path || saved.SHA256 != expected.SHA256 || saved.Size != expected.Size || saved.Parts != expected.Parts || len(saved.Done) != expected.Parts {
		return false
	}
	if dst != nil {
		*dst = saved
	}
	return true
}

func writePartState(path string, state partState) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	err = json.NewEncoder(f).Encode(state)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	err = dir.Sync()
	if closeErr := dir.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (d *Downloader) downloadParallel(ctx context.Context, repo, revision, part string, f repoFile, slots chan struct{}, fileProgress *fileProgress, progress *downloadProgress) error {
	parts := min(d.Parts, int((f.Size+(8<<20)-1)/(8<<20)))
	if parts < 2 {
		return d.downloadAttemptWithSlots(ctx, repo, revision, part, f, slots, fileProgress, progress)
	}
	state := newPartState(repo, revision, f, parts)
	meta := part + ".meta"
	matched := loadMatchingPartState(meta, state, &state)
	out, err := os.OpenFile(part, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer out.Close()
	if st, err := out.Stat(); err != nil || st.Size() != f.Size || !matched {
		state.Done = make([]bool, parts)
		if err := out.Truncate(f.Size); err != nil {
			return err
		}
		if err := writePartState(meta, state); err != nil {
			return fmt.Errorf("保存续传状态: %w", err)
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	chunk := (f.Size + int64(parts) - 1) / int64(parts)
	var completed int64
	for i, done := range state.Done {
		if done {
			completed += min(chunk, f.Size-int64(i)*chunk)
		}
	}
	fileProgress.set(completed)
	for i := 0; i < parts; i++ {
		if state.Done[i] {
			continue
		}
		start := int64(i) * chunk
		end := min(start+chunk, f.Size) - 1
		wg.Add(1)
		go func(index int, start, end int64) {
			defer wg.Done()
			if err := acquire(ctx, slots); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				return
			}
			defer release(slots)
			progress.connection(1)
			defer progress.connection(-1)
			if err := d.downloadRange(ctx, repo, revision, out, f.Path, start, end, fileProgress); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				cancel()
				return
			}
			mu.Lock()
			if err := out.Sync(); err != nil {
				errs = append(errs, err)
				cancel()
			} else {
				state.Done[index] = true
			}
			if len(errs) == 0 {
				if err := writePartState(meta, state); err != nil {
					errs = append(errs, err)
					cancel()
				}
			}
			mu.Unlock()
		}(i, start, end)
	}
	wg.Wait()
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return out.Sync()
}

func responseValidator(resp *http.Response) string {
	if etag := resp.Header.Get("ETag"); etag != "" {
		return etag
	}
	return resp.Header.Get("Last-Modified")
}

func validContentRange(value string, offset, size int64) bool {
	var start, end, total int64
	if _, err := fmt.Sscanf(value, "bytes %d-%d/%d", &start, &end, &total); err != nil {
		return false
	}
	return start == offset && end >= start && (size <= 0 || total == size)
}

func (d *Downloader) downloadRange(ctx context.Context, repo, revision string, out *os.File, path string, start, end int64, fileProgress *fileProgress) error {
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	timer := time.AfterFunc(d.Timeout, cancel)
	defer timer.Stop()
	u := d.Endpoint + "/api/v1/models/" + escapeRepo(repo) + "/repo"
	q := url.Values{"Revision": {revision}, "FilePath": {path}}
	req, err := http.NewRequestWithContext(attemptCtx, http.MethodGet, u+"?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	d.headers(req)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	resp, err := d.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	wantPrefix := fmt.Sprintf("bytes %d-%d/", start, end)
	if resp.StatusCode != http.StatusPartialContent || !strings.HasPrefix(resp.Header.Get("Content-Range"), wantPrefix) {
		return errRangeUnsupported
	}
	written, err := io.CopyBuffer(io.NewOffsetWriter(out, start), &activityReader{r: io.LimitReader(resp.Body, end-start+1), timer: timer, timeout: d.Timeout, onRead: fileProgress.read}, make([]byte, 1<<20))
	if err != nil {
		return err
	}
	if written != end-start+1 {
		return fmt.Errorf("分片不完整: 得到 %d 字节，预期 %d", written, end-start+1)
	}
	return nil
}

func acquire(ctx context.Context, slots chan struct{}) error {
	if slots == nil {
		return nil
	}
	select {
	case slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func release(slots chan struct{}) {
	if slots != nil {
		<-slots
	}
}

func (d *Downloader) validExisting(path string, f repoFile) (bool, error) {
	st, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	if f.Size > 0 && st.Size() != f.Size {
		return false, nil
	}
	if !d.Verify || f.SHA256 == "" {
		return true, nil
	}
	return hashMatches(path, f.SHA256)
}

func verifyFile(path string, f repoFile, verify bool) error {
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	if f.Size > 0 && st.Size() != f.Size {
		return fmt.Errorf("文件不完整: 得到 %d 字节，预期 %d", st.Size(), f.Size)
	}
	if verify && f.SHA256 != "" {
		ok, err := hashMatches(path, f.SHA256)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("SHA-256 校验失败")
		}
	}
	return nil
}

func hashMatches(path, expected string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.CopyBuffer(h, f, make([]byte, 1<<20)); err != nil {
		return false, err
	}
	return strings.EqualFold(hex.EncodeToString(h.Sum(nil)), expected), nil
}

func (d *Downloader) client() *http.Client {
	if d.Client != nil {
		return d.Client
	}
	return &http.Client{Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		ResponseHeaderTimeout: d.Timeout,
		MaxIdleConnsPerHost:   max(2, d.Workers),
	}}
}

type activityReader struct {
	r       io.Reader
	timer   *time.Timer
	timeout time.Duration
	onRead  func(int)
}

func (r *activityReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 {
		r.timer.Reset(r.timeout)
		if r.onRead != nil {
			r.onRead(n)
		}
	}
	return n, err
}
func (d *Downloader) writer() io.Writer {
	if d.Out != nil {
		return d.Out
	}
	return io.Discard
}
func (d *Downloader) headers(req *http.Request) {
	req.Header.Set("User-Agent", "msget/"+version)
	if d.Token != "" {
		req.Header.Set("Authorization", "Bearer "+d.Token)
		req.Header.Set("X-ModelScope-Token", d.Token)
	}
}

func responseError(action string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	message := strings.TrimSpace(string(body))
	if message == "" {
		message = resp.Status
	}
	return fmt.Errorf("%s: HTTP %d: %s", action, resp.StatusCode, message)
}

func validateRepo(repo string) error {
	parts := strings.Split(repo, "/")
	if len(parts) < 2 {
		return errors.New("模型 ID 应为 namespace/model，例如 Qwen/Qwen3-0.6B")
	}
	for _, p := range parts {
		if p == "" || p == "." || p == ".." {
			return errors.New("模型 ID 非法")
		}
	}
	return nil
}
func escapeRepo(repo string) string {
	parts := strings.Split(repo, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	return strings.Join(parts, "/")
}
func safeTarget(root, remote string) (string, error) {
	remote = strings.ReplaceAll(remote, "\\", "/")
	clean := filepath.Clean(filepath.FromSlash(remote))
	if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", errors.New("路径越界")
	}
	target := filepath.Join(root, clean)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("路径越界")
	}
	return target, nil
}
func selectedBy(name string, includes, excludes []string) bool {
	if len(includes) > 0 && !matchesAny(name, includes) {
		return false
	}
	return !matchesAny(name, excludes)
}
func matchesAny(name string, patterns []string) bool {
	for _, pattern := range patterns {
		re, err := regexp.Compile(globRegexp(pattern))
		if err == nil && re.MatchString(name) {
			return true
		}
	}
	return false
}
func globRegexp(glob string) string {
	var b strings.Builder
	b.WriteByte('^')
	for i := 0; i < len(glob); i++ {
		switch glob[i] {
		case '*':
			if i+1 < len(glob) && glob[i+1] == '*' {
				b.WriteString(".*")
				i++
			} else {
				b.WriteString("[^/]*")
			}
		case '?':
			b.WriteString("[^/]")
		case '/':
			b.WriteByte('/')
		default:
			b.WriteString(regexp.QuoteMeta(string(glob[i])))
		}
	}
	b.WriteByte('$')
	return b.String()
}
func humanBytes(n int64) string {
	const unit = int64(1024)
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := unit, 0
	for value := n / unit; value >= unit && exp < 5; value /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
