package downloader

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestWriter(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	if got := (&Downloader{Out: &out}).writer(); got != &out {
		t.Fatal("writer did not return configured output")
	}
	if _, err := (&Downloader{}).writer().Write([]byte("discarded")); err != nil {
		t.Fatal(err)
	}
}

func TestHeaders(t *testing.T) {
	t.Parallel()
	req, err := http.NewRequest(http.MethodGet, "https://example.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	d := Downloader{Token: "secret"}
	d.headers(req)
	if got := req.Header.Get("Authorization"); got != "Bearer secret" {
		t.Errorf("Authorization = %q", got)
	}
	if got := req.Header.Get("X-ModelScope-Token"); got != "secret" {
		t.Errorf("X-ModelScope-Token = %q", got)
	}
	if got := req.Header.Get("User-Agent"); !strings.HasPrefix(got, "msget/") {
		t.Errorf("User-Agent = %q", got)
	}
}

func TestResponseError(t *testing.T) {
	t.Parallel()
	resp := &http.Response{
		StatusCode: http.StatusForbidden,
		Status:     "403 Forbidden",
		Body:       io.NopCloser(strings.NewReader(" denied \n")),
	}
	if got := responseError("download", resp).Error(); got != "download: HTTP 403: denied" {
		t.Fatalf("responseError() = %q", got)
	}
}

func TestResponseErrorUsesStatusForEmptyBody(t *testing.T) {
	t.Parallel()
	resp := &http.Response{
		StatusCode: http.StatusBadGateway,
		Status:     "502 Bad Gateway",
		Body:       io.NopCloser(strings.NewReader("")),
	}
	if got := responseError("list files", resp).Error(); got != "list files: HTTP 502: 502 Bad Gateway" {
		t.Fatalf("responseError() = %q", got)
	}
}

func TestValidateRepo(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		repo    string
		wantErr bool
	}{
		{"Qwen/Qwen3", false},
		{"org/group/model", false},
		{"model", true},
		{"/model", true},
		{"org/../model", true},
	} {
		if err := validateRepo(tt.repo); (err != nil) != tt.wantErr {
			t.Errorf("validateRepo(%q) error = %v, wantErr %v", tt.repo, err, tt.wantErr)
		}
	}
}

func TestResponseValidator(t *testing.T) {
	t.Parallel()
	resp := &http.Response{Header: make(http.Header)}
	resp.Header.Set("Last-Modified", "yesterday")
	if got := responseValidator(resp); got != "yesterday" {
		t.Fatalf("validator = %q", got)
	}
	resp.Header.Set("ETag", `"v2"`)
	if got := responseValidator(resp); got != `"v2"` {
		t.Fatalf("ETag should take precedence, got %q", got)
	}
}

func TestValidContentRange(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		value        string
		offset, size int64
		want         bool
	}{
		{"bytes 4-9/10", 4, 10, true},
		{"bytes 3-9/10", 4, 10, false},
		{"bytes 4-9/11", 4, 10, false},
		{"invalid", 4, 10, false},
	} {
		if got := validContentRange(tt.value, tt.offset, tt.size); got != tt.want {
			t.Errorf("validContentRange(%q, %d, %d) = %v, want %v", tt.value, tt.offset, tt.size, got, tt.want)
		}
	}
}
