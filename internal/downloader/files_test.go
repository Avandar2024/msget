package downloader

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestVerifyFile(t *testing.T) {
	t.Parallel()
	content := []byte("model")
	path := filepath.Join(t.TempDir(), "model.bin")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(content))

	tests := []struct {
		name    string
		file    repoFile
		verify  bool
		wantErr bool
	}{
		{name: "matching metadata", file: repoFile{Size: int64(len(content)), SHA256: hash}, verify: true},
		{name: "wrong size", file: repoFile{Size: int64(len(content) + 1)}, wantErr: true},
		{name: "wrong checksum", file: repoFile{Size: int64(len(content)), SHA256: "deadbeef"}, verify: true, wantErr: true},
		{name: "checksum disabled", file: repoFile{Size: int64(len(content)), SHA256: "deadbeef"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := verifyFile(path, tt.file, tt.verify)
			if (err != nil) != tt.wantErr {
				t.Fatalf("verifyFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidExisting(t *testing.T) {
	t.Parallel()
	content := []byte("model")
	path := filepath.Join(t.TempDir(), "model.bin")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(content))
	d := Downloader{Verify: true}
	if ok, err := d.validExisting(path, repoFile{Size: int64(len(content)), SHA256: hash}); err != nil || !ok {
		t.Fatalf("validExisting() = %v, %v", ok, err)
	}
	if ok, err := d.validExisting(path, repoFile{Size: 999}); err != nil || ok {
		t.Fatalf("wrong-size validExisting() = %v, %v", ok, err)
	}
	if ok, err := d.validExisting(filepath.Join(t.TempDir(), "missing"), repoFile{}); err == nil || ok {
		t.Fatalf("missing validExisting() = %v, %v", ok, err)
	}
}

func TestGlobPatterns(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, input, pattern string
		want                 bool
	}{
		{name: "single star", input: "dir/a.json", pattern: "*.json", want: false},
		{name: "double star", input: "dir/a.json", pattern: "**/*.json", want: true},
		{name: "question mark", input: "dir/a.json", pattern: "dir/a.jso?", want: true},
		{name: "literal punctuation", input: "dir/a.json", pattern: "dir/a.json", want: true},
		{name: "star within segment", input: "dir/a.json", pattern: "dir/*.json", want: true},
		{name: "star cannot cross slash", input: "dir/a.json", pattern: "dir*json", want: false},
		{name: "double star crosses slash", input: "dir/a.json", pattern: "**json", want: true},
		{name: "question cannot cross slash", input: "dir/a.json", pattern: "dir?a.json", want: false},
		{name: "unicode question", input: "dir/模.json", pattern: "dir/?.json", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchesAny(tt.input, []string{tt.pattern}); got != tt.want {
				t.Fatalf("matchesAny() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHumanBytes(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		n    int64
		want string
	}{
		{0, "0 B"},
		{1023, "1023 B"},
		{1024, "1.0 KiB"},
		{5 << 20, "5.0 MiB"},
	} {
		if got := humanBytes(tt.n); got != tt.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}
