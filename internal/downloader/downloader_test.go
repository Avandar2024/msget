package downloader

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDownload(t *testing.T) {
	body := []byte("model data")
	hash := fmt.Sprintf("%x", sha256.Sum256(body))
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/models/acme/model/repo/files":
			fmt.Fprintf(w, `{"Code":200,"Data":{"Files":[{"Path":"weights/model.bin","Type":"blob","Size":%d,"Sha256":"%s"}]}}`, len(body), hash)
		case "/api/v1/models/acme/model/repo":
			w.Write(body)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	out := t.TempDir()
	d := Downloader{Endpoint: server.URL, Workers: 2, Retries: 1, Timeout: time.Second, Verify: true, Out: io.Discard}
	if err := d.Download(context.Background(), "acme/model", "master", out, nil, nil); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(out, "weights", "model.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(body) {
		t.Fatalf("got %q", got)
	}
}

func TestListErrors(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{name: "HTTP error", status: http.StatusUnauthorized, body: "token required", want: "HTTP 401"},
		{name: "invalid JSON", status: http.StatusOK, body: "not json", want: "decode file list"},
		{name: "API error", status: http.StatusOK, body: `{"Code":500,"Message":"backend failed"}`, want: "backend failed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			}))
			defer server.Close()
			d := Downloader{Endpoint: server.URL, Client: server.Client()}
			_, err := d.list(context.Background(), "org/model", "main")
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("list() error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestDownloadReportsNoMatchingFiles(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"Code":200,"Data":{"Files":[{"Path":"model.bin","Type":"blob","Size":10}]}}`)
	}))
	defer server.Close()
	d := Downloader{Endpoint: server.URL, Client: server.Client()}
	err := d.Download(context.Background(), "org/model", "main", t.TempDir(), []string{"*.json"}, nil)
	if err == nil || !strings.Contains(err.Error(), "no model files matched") {
		t.Fatalf("Download() error = %v", err)
	}
}

func TestDownloadRejectsUnsafeServerPath(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"Code":200,"Data":{"Files":[{"Path":"../escape","Type":"blob","Size":1}]}}`)
	}))
	defer server.Close()
	d := Downloader{Endpoint: server.URL, Client: server.Client()}
	err := d.Download(context.Background(), "org/model", "main", t.TempDir(), nil, nil)
	if err == nil || !strings.Contains(err.Error(), "unsafe path") {
		t.Fatalf("Download() error = %v", err)
	}
}

func TestResume(t *testing.T) {
	body := []byte("0123456789")
	var gotRange string
	var gotIfRange string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRange = r.Header.Get("Range")
		gotIfRange = r.Header.Get("If-Range")
		w.Header().Set("Content-Range", "bytes 4-9/10")
		w.WriteHeader(http.StatusPartialContent)
		w.Write(body[4:])
	}))
	defer server.Close()
	dir := t.TempDir()
	part := filepath.Join(dir, "file.part")
	if err := os.WriteFile(part, body[:4], 0o644); err != nil {
		t.Fatal(err)
	}
	state := newPartState("a/b", "master", repoFile{Path: "file", Size: 10}, 1)
	state.Validator = `"version-1"`
	if err := writePartState(part+".meta", state); err != nil {
		t.Fatal(err)
	}
	d := Downloader{Endpoint: server.URL, Timeout: time.Second}
	if err := d.downloadAttempt(context.Background(), "a/b", "master", part, repoFile{Path: "file", Size: 10}); err != nil {
		t.Fatal(err)
	}
	if gotRange != "bytes=4-" {
		t.Fatalf("Range = %q", gotRange)
	}
	if gotIfRange != `"version-1"` {
		t.Fatalf("If-Range = %q", gotIfRange)
	}
	got, _ := os.ReadFile(part)
	if string(got) != string(body) {
		t.Fatalf("got %q", got)
	}
}

func TestResumeRejectsDifferentRevision(t *testing.T) {
	body := []byte("new content")
	var gotRange string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRange = r.Header.Get("Range")
		_, _ = w.Write(body)
	}))
	defer server.Close()
	part := filepath.Join(t.TempDir(), "file.part")
	if err := os.WriteFile(part, []byte("old "), 0o644); err != nil {
		t.Fatal(err)
	}
	old := newPartState("a/b", "old-revision", repoFile{Path: "file", Size: int64(len(body))}, 1)
	if err := writePartState(part+".meta", old); err != nil {
		t.Fatal(err)
	}
	d := Downloader{Endpoint: server.URL, Timeout: time.Second, Client: server.Client()}
	if err := d.downloadAttempt(context.Background(), "a/b", "new-revision", part, repoFile{Path: "file", Size: int64(len(body))}); err != nil {
		t.Fatal(err)
	}
	if gotRange != "" {
		t.Fatalf("stale partial file was resumed with %q", gotRange)
	}
	got, _ := os.ReadFile(part)
	if string(got) != string(body) {
		t.Fatalf("got %q", got)
	}
}

func TestParallelRanges(t *testing.T) {
	body := make([]byte, 16<<20+1)
	for i := range body {
		body[i] = byte(i % 251)
	}
	var mu sync.Mutex
	var ranges []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := strings.TrimPrefix(r.Header.Get("Range"), "bytes=")
		bounds := strings.Split(h, "-")
		if len(bounds) != 2 {
			http.Error(w, "range required", http.StatusBadRequest)
			return
		}
		start, err1 := strconv.ParseInt(bounds[0], 10, 64)
		end, err2 := strconv.ParseInt(bounds[1], 10, 64)
		if err1 != nil || err2 != nil || start < 0 || end >= int64(len(body)) || start > end {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		mu.Lock()
		ranges = append(ranges, r.Header.Get("Range"))
		mu.Unlock()
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[start : end+1])
	}))
	defer server.Close()

	part := filepath.Join(t.TempDir(), "model.part")
	d := Downloader{Endpoint: server.URL, Workers: 3, Parts: 3, RangeSize: 4 << 20, Timeout: 5 * time.Second, Client: server.Client()}
	progress := newDownloadProgress(io.Discard, int64(len(body)), 1)
	if err := d.downloadParallel(context.Background(), "a/b", "master", part, repoFile{Path: "model", Size: int64(len(body))}, make(chan struct{}, 3), progress.file(int64(len(body))), progress); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(part)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(fmt.Sprintf("%x", sha256.Sum256(got)), fmt.Sprintf("%x", sha256.Sum256(body))) {
		t.Fatal("parallel download content mismatch")
	}
	if len(ranges) <= d.Parts {
		t.Fatalf("got %d range request(s), want more than %d connections", len(ranges), d.Parts)
	}
}

func TestParallelLayoutUsesDynamicRanges(t *testing.T) {
	t.Parallel()
	connections, ranges := parallelLayout(1<<30, 4, 64<<20)
	if connections != 4 || ranges != 16 {
		t.Fatalf("layout = %d connections, %d ranges; want 4, 16", connections, ranges)
	}
}

func TestParallelRetriesOnlyFailedRange(t *testing.T) {
	body := make([]byte, 16<<20)
	for i := range body {
		body[i] = byte(i % 251)
	}
	chunk := int64(len(body)) / 2
	mid := chunk / 2
	var mu sync.Mutex
	requests := make(map[string]int)
	failed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		bounds := strings.Split(strings.TrimPrefix(rangeHeader, "bytes="), "-")
		start, _ := strconv.ParseInt(bounds[0], 10, 64)
		end, _ := strconv.ParseInt(bounds[1], 10, 64)
		mu.Lock()
		requests[rangeHeader]++
		failThis := start == 0 && !failed
		if failThis {
			failed = true
		}
		mu.Unlock()
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
		if failThis {
			_, _ = w.Write(body[start:mid])
			return
		}
		_, _ = w.Write(body[start : end+1])
	}))
	defer server.Close()

	part := filepath.Join(t.TempDir(), "model.part")
	d := Downloader{Endpoint: server.URL, Workers: 2, Parts: 2, Retries: 1, Timeout: 5 * time.Second, Client: server.Client()}
	progress := newDownloadProgress(io.Discard, int64(len(body)), 1)
	if err := d.downloadParallel(context.Background(), "a/b", "master", part, repoFile{Path: "model", Size: int64(len(body))}, make(chan struct{}, 2), progress.file(int64(len(body))), progress); err != nil {
		t.Fatal(err)
	}
	if requests[fmt.Sprintf("bytes=%d-%d", chunk, int64(len(body))-1)] != 1 {
		t.Fatalf("successful range was retried: %v", requests)
	}
	if requests[fmt.Sprintf("bytes=%d-%d", mid, chunk-1)] != 1 {
		t.Fatalf("failed range did not resume independently: %v", requests)
	}
	got, _ := os.ReadFile(part)
	if !slices.Equal(got, body) {
		t.Fatal("retried download content mismatch")
	}
}

func TestParallelResumeSkipsCompletedParts(t *testing.T) {
	body := make([]byte, 16<<20+1)
	for i := range body {
		body[i] = byte(i % 251)
	}
	var mu sync.Mutex
	var ranges []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := strings.TrimPrefix(r.Header.Get("Range"), "bytes=")
		bounds := strings.Split(h, "-")
		start, _ := strconv.ParseInt(bounds[0], 10, 64)
		end, _ := strconv.ParseInt(bounds[1], 10, 64)
		mu.Lock()
		ranges = append(ranges, r.Header.Get("Range"))
		mu.Unlock()
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[start : end+1])
	}))
	defer server.Close()

	part := filepath.Join(t.TempDir(), "model.part")
	parts := 3
	chunk := (int64(len(body)) + int64(parts) - 1) / int64(parts)
	f, err := os.OpenFile(part, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(int64(len(body))); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(body[:chunk], 0); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	file := repoFile{Path: "model", Size: int64(len(body)), SHA256: "identity"}
	state := newPartState("a/b", "master", file, parts)
	state.Done[0] = true
	if err := writePartState(part+".meta", state); err != nil {
		t.Fatal(err)
	}
	d := Downloader{Endpoint: server.URL, Workers: parts, Parts: parts, Timeout: 5 * time.Second, Client: server.Client()}
	progress := newDownloadProgress(io.Discard, int64(len(body)), 1)
	if err := d.downloadParallel(context.Background(), "a/b", "master", part, file, make(chan struct{}, parts), progress.file(int64(len(body))), progress); err != nil {
		t.Fatal(err)
	}
	if len(ranges) != parts-1 {
		t.Fatalf("got %d requests, want %d unfinished parts", len(ranges), parts-1)
	}
	got, _ := os.ReadFile(part)
	if fmt.Sprintf("%x", sha256.Sum256(got)) != fmt.Sprintf("%x", sha256.Sum256(body)) {
		t.Fatal("resumed download content mismatch")
	}
}

func TestParallelResumeContinuesPartialParts(t *testing.T) {
	body := make([]byte, 16<<20+1)
	for i := range body {
		body[i] = byte(i % 251)
	}
	var mu sync.Mutex
	var ranges []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bounds := strings.Split(strings.TrimPrefix(r.Header.Get("Range"), "bytes="), "-")
		start, _ := strconv.ParseInt(bounds[0], 10, 64)
		end, _ := strconv.ParseInt(bounds[1], 10, 64)
		mu.Lock()
		ranges = append(ranges, r.Header.Get("Range"))
		mu.Unlock()
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[start : end+1])
	}))
	defer server.Close()

	part := filepath.Join(t.TempDir(), "model.part")
	parts := 3
	chunk := (int64(len(body)) + int64(parts) - 1) / int64(parts)
	received := int64(12345)
	f, err := os.OpenFile(part, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(int64(len(body))); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(body[:received], 0); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	file := repoFile{Path: "model", Size: int64(len(body)), SHA256: "identity"}
	state := newPartState("a/b", "master", file, parts)
	state.Received[0] = received
	if err := writePartState(part+".meta", state); err != nil {
		t.Fatal(err)
	}
	d := Downloader{Endpoint: server.URL, Workers: parts, Parts: parts, Timeout: 5 * time.Second, Client: server.Client()}
	progress := newDownloadProgress(io.Discard, int64(len(body)), 1)
	if err := d.downloadParallel(context.Background(), "a/b", "master", part, file, make(chan struct{}, parts), progress.file(int64(len(body))), progress); err != nil {
		t.Fatal(err)
	}
	wantRange := fmt.Sprintf("bytes=%d-%d", received, chunk-1)
	if !slices.Contains(ranges, wantRange) {
		t.Fatalf("ranges %q do not continue partial range at %q", ranges, wantRange)
	}
	got, _ := os.ReadFile(part)
	if fmt.Sprintf("%x", sha256.Sum256(got)) != fmt.Sprintf("%x", sha256.Sum256(body)) {
		t.Fatal("partial-range resumed content mismatch")
	}
}

func TestSafeTarget(t *testing.T) {
	root := t.TempDir()
	for _, p := range []string{"../evil", "a/../../evil", "/etc/passwd", `..\\evil`} {
		if _, err := safeTarget(root, p); err == nil {
			t.Errorf("accepted %q", p)
		}
	}
	if got, err := safeTarget(root, "a/b.bin"); err != nil || !strings.HasPrefix(got, root) {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestPatterns(t *testing.T) {
	if !selectedBy("dir/a.json", []string{"**/*.json"}, nil) {
		t.Fatal("include did not match")
	}
	if selectedBy("dir/a.bin", nil, []string{"**/*.bin"}) {
		t.Fatal("exclude did not match")
	}
}

func TestHTTPTransportPool(t *testing.T) {
	d := Downloader{Workers: 7, Timeout: 45 * time.Second, IdleConnTimeout: 2 * time.Minute, Network: NetworkAuto}
	client := d.client()
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T", client.Transport)
	}
	if transport.MaxConnsPerHost != 7 || transport.MaxIdleConnsPerHost != 7 {
		t.Fatalf("per-host limits = active %d, idle %d", transport.MaxConnsPerHost, transport.MaxIdleConnsPerHost)
	}
	if transport.MaxIdleConns != 16 {
		t.Fatalf("MaxIdleConns = %d", transport.MaxIdleConns)
	}
	if transport.IdleConnTimeout != 2*time.Minute {
		t.Fatalf("IdleConnTimeout = %s", transport.IdleConnTimeout)
	}
	if transport.DialContext == nil || !transport.ForceAttemptHTTP2 {
		t.Fatal("TCP dialer or HTTP/2 was not configured")
	}
}
