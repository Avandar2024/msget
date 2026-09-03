package downloader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type Downloader struct {
	Endpoint        string
	Token           string
	UserAgent       string
	Workers         int
	Parts           int
	Retries         int
	Timeout         time.Duration
	IdleConnTimeout time.Duration
	Network         string
	Verify          bool
	Out             io.Writer
	Client          *http.Client
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
		defer copy.Client.CloseIdleConnections()
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
			return fmt.Errorf("server returned unsafe path %q: %w", f.Path, err)
		}
		selected = append(selected, f)
		total += f.Size
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Path < selected[j].Path })
	if len(selected) == 0 {
		return errors.New("no model files matched")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	fmt.Fprintf(d.writer(), "Model %s@%s: %d files, %s -> %s\n", repo, revision, len(selected), humanBytes(total), root)
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
				progress.fileActive(f.Path, true)
				if err := d.downloadFile(ctx, repo, revision, root, f, slots, progress); err != nil {
					if ctx.Err() == nil {
						mu.Lock()
						failures = append(failures, fmt.Errorf("%s: %w", f.Path, err))
						progress.log("Failed  %s: %v\n", f.Path, err)
						mu.Unlock()
					}
				} else {
					mu.Lock()
					progress.logDone("Done  %s (%s)\n", f.Path, humanBytes(f.Size))
					mu.Unlock()
				}
				progress.fileActive(f.Path, false)
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
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d file(s) failed (run again to resume): %w", len(failures), errors.Join(failures...))
	}
	progress.finish()
	fmt.Fprintf(d.writer(), "Download complete: %s\n", root)
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
		return nil, fmt.Errorf("list files: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, responseError("list files", resp)
	}
	var result listResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode file list: %w", err)
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
		return fmt.Errorf("save resume state: %w", err)
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
		return responseError("download", resp)
	}
	appendMode := offset > 0 && resp.StatusCode == http.StatusPartialContent && validContentRange(resp.Header.Get("Content-Range"), offset, f.Size)
	if offset > 0 && resp.StatusCode == http.StatusPartialContent && !appendMode {
		return fmt.Errorf("server returned mismatched Content-Range: %q", resp.Header.Get("Content-Range"))
	}
	if validator := responseValidator(resp); validator != "" && validator != state.Validator {
		state.Validator = validator
		if err := writePartState(meta, state); err != nil {
			return fmt.Errorf("save resume state: %w", err)
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

var errRangeUnsupported = errors.New("server does not support ranged downloads")

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
		state.Received = make([]int64, parts)
		if err := out.Truncate(f.Size); err != nil {
			return err
		}
		if err := writePartState(meta, state); err != nil {
			return fmt.Errorf("save resume state: %w", err)
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	chunk := (f.Size + int64(parts) - 1) / int64(parts)
	var completed int64
	for i := range state.Done {
		partSize := min(chunk, f.Size-int64(i)*chunk)
		if state.Done[i] {
			state.Received[i] = partSize
		}
		state.Received[i] = max(int64(0), min(state.Received[i], partSize))
		completed += state.Received[i]
	}
	fileProgress.set(completed)
	for i := 0; i < parts; i++ {
		if state.Done[i] {
			continue
		}
		partStart := int64(i) * chunk
		start := partStart + state.Received[i]
		end := min(partStart+chunk, f.Size) - 1
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
			written, downloadErr := d.downloadRange(ctx, repo, revision, out, f.Path, start, end, fileProgress)
			mu.Lock()
			if err := out.Sync(); err != nil {
				errs = append(errs, err)
				cancel()
			} else {
				state.Received[index] += written
				state.Done[index] = state.Received[index] == end-(int64(index)*chunk)+1
			}
			if err := writePartState(meta, state); err != nil {
				errs = append(errs, err)
				cancel()
			}
			if downloadErr != nil {
				errs = append(errs, downloadErr)
				cancel()
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

func (d *Downloader) downloadRange(ctx context.Context, repo, revision string, out *os.File, path string, start, end int64, fileProgress *fileProgress) (int64, error) {
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	timer := time.AfterFunc(d.Timeout, cancel)
	defer timer.Stop()
	u := d.Endpoint + "/api/v1/models/" + escapeRepo(repo) + "/repo"
	q := url.Values{"Revision": {revision}, "FilePath": {path}}
	req, err := http.NewRequestWithContext(attemptCtx, http.MethodGet, u+"?"+q.Encode(), nil)
	if err != nil {
		return 0, err
	}
	d.headers(req)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	resp, err := d.client().Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	wantPrefix := fmt.Sprintf("bytes %d-%d/", start, end)
	if resp.StatusCode != http.StatusPartialContent || !strings.HasPrefix(resp.Header.Get("Content-Range"), wantPrefix) {
		return 0, errRangeUnsupported
	}
	written, err := io.CopyBuffer(io.NewOffsetWriter(out, start), &activityReader{r: io.LimitReader(resp.Body, end-start+1), timer: timer, timeout: d.Timeout, onRead: fileProgress.read}, make([]byte, 1<<20))
	if err != nil {
		return written, err
	}
	if written != end-start+1 {
		return written, fmt.Errorf("incomplete range: got %d bytes, expected %d", written, end-start+1)
	}
	return written, nil
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
