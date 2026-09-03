package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

func TestResume(t *testing.T) {
	body := []byte("0123456789")
	var gotRange string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRange = r.Header.Get("Range")
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
	d := Downloader{Endpoint: server.URL, Timeout: time.Second}
	if err := d.downloadAttempt(context.Background(), "a/b", "master", part, repoFile{Path: "file", Size: 10}); err != nil {
		t.Fatal(err)
	}
	if gotRange != "bytes=4-" {
		t.Fatalf("Range = %q", gotRange)
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
	d := Downloader{Endpoint: server.URL, Workers: 3, Parts: 3, Timeout: 5 * time.Second, Client: server.Client()}
	if err := d.downloadParallel(context.Background(), "a/b", "master", part, repoFile{Path: "model", Size: int64(len(body))}, make(chan struct{}, 3)); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(part)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(fmt.Sprintf("%x", sha256.Sum256(got)), fmt.Sprintf("%x", sha256.Sum256(body))) {
		t.Fatal("parallel download content mismatch")
	}
	if len(ranges) < 2 {
		t.Fatalf("got %d range request(s), want at least 2", len(ranges))
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
