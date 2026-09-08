package downloader

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHFDownload(t *testing.T) {
	for _, size := range []int{12, 16 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			body := bytes.Repeat([]byte("x"), size)
			hash := fmt.Sprintf("%x", sha256.Sum256(body))
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer hf-test" || r.Header.Get("X-ModelScope-Token") != "" {
					t.Error("incorrect HF authentication")
				}
				switch r.URL.Path {
				case "/api/models/acme/model/revision/main":
					if r.URL.Query().Get("blobs") != "true" {
						t.Error("missing file metadata")
					}
					fmt.Fprintf(w, `{"siblings":[{"rfilename":"sub/model.bin","size":1,"lfs":{"size":%d,"sha256":"%s"}},{"rfilename":"skip.json","size":2}]}`, size, hash)
				case "/acme/model/resolve/main/sub/model.bin":
					http.ServeContent(w, r, "model.bin", time.Time{}, bytes.NewReader(body))
				default:
					t.Errorf("unexpected URL: %s", r.URL)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			d := Downloader{Source: SourceHF, Endpoint: server.URL, Token: "hf-test", Workers: 2, Parts: 2, Timeout: time.Second * 10, Verify: true, Out: io.Discard}
			out := t.TempDir()
			if err := d.Download(context.Background(), "acme/model", "", out, []string{"**/*.bin"}, nil); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(filepath.Join(out, "sub/model.bin"))
			if err != nil || !bytes.Equal(got, body) {
				t.Fatalf("download mismatch: %v", err)
			}
		})
	}
}

func TestHFListErrors(t *testing.T) {
	for _, tc := range []struct {
		body   string
		status int
		want   string
	}{
		{"denied", 401, "HTTP 401"},
		{"invalid", 200, "decode Hugging Face file list"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status); fmt.Fprint(w, tc.body) }))
			defer s.Close()
			d := Downloader{Source: SourceHF, Endpoint: s.URL}
			_, err := d.list(context.Background(), "acme/model", "main")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestHFURLAndResumeIsolation(t *testing.T) {
	d := Downloader{Source: SourceHF, Endpoint: "https://hf-mirror.com"}
	if got := d.fileURL("acme/model", "refs/pr/1", "sub/a #?.bin"); got != "https://hf-mirror.com/acme/model/resolve/refs%2Fpr%2F1/sub/a%20%23%3F.bin" {
		t.Fatal(got)
	}
	f := repoFile{Path: "file", Size: 10}
	old := newPartState("acme/model", "main", f, 1)
	path := filepath.Join(t.TempDir(), "state")
	if err := writePartState(path, old); err != nil {
		t.Fatal(err)
	}
	want := newPartState(d.resumeRepo("acme/model"), "main", f, 1)
	if loadMatchingPartState(path, want, nil) {
		t.Fatal("reused ModelScope checkpoint")
	}
}

func TestUnknownSource(t *testing.T) {
	d := Downloader{Source: "invalid"}
	if err := d.Download(context.Background(), "acme/model", "", t.TempDir(), nil, nil); err == nil {
		t.Fatal("expected invalid source error")
	}
}

func TestAutoFallsBackToHFOnModelScopeNotFound(t *testing.T) {
	body := []byte("from hf mirror")
	modelScopeCalls := 0
	modelScope := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		modelScopeCalls++
		if r.Header.Get("Authorization") != "Bearer ms-test" {
			t.Error("missing ModelScope token")
		}
		http.NotFound(w, r)
	}))
	defer modelScope.Close()

	hfCalls := 0
	hf := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hfCalls++
		if r.Header.Get("Authorization") != "Bearer hf-test" || r.Header.Get("X-ModelScope-Token") != "" {
			t.Error("incorrect HF token headers")
		}
		switch r.URL.Path {
		case "/api/models/acme/model/revision/main":
			fmt.Fprintf(w, `{"siblings":[{"rfilename":"model.bin","size":%d}]}`, len(body))
		case "/acme/model/resolve/main/model.bin":
			_, _ = w.Write(body)
		default:
			http.NotFound(w, r)
		}
	}))
	defer hf.Close()

	out := t.TempDir()
	d := Downloader{
		Source: SourceAuto, Endpoint: modelScope.URL, Token: "ms-test",
		HFEndpoint: hf.URL, HFToken: "hf-test", Workers: 1, Parts: 1,
		Retries: 0, Timeout: time.Second, Verify: true, Out: io.Discard,
	}
	if err := d.Download(context.Background(), "acme/model", "", out, nil, nil); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(out, "model.bin"))
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("download mismatch: %q, %v", got, err)
	}
	if modelScopeCalls != 1 || hfCalls != 2 {
		t.Fatalf("calls: ModelScope=%d HF=%d", modelScopeCalls, hfCalls)
	}
}

func TestAutoDoesNotFallbackOnModelScopeFailure(t *testing.T) {
	modelScope := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer modelScope.Close()
	hfCalled := false
	hf := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hfCalled = true }))
	defer hf.Close()

	d := Downloader{Source: SourceAuto, Endpoint: modelScope.URL, HFEndpoint: hf.URL, Workers: 1, Out: io.Discard}
	err := d.Download(context.Background(), "acme/model", "", t.TempDir(), nil, nil)
	if err == nil || !strings.Contains(err.Error(), "HTTP 503") {
		t.Fatalf("got %v", err)
	}
	if hfCalled {
		t.Fatal("HF fallback was called for a non-404 error")
	}
}
