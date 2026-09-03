package downloader

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestProgressShowsActiveFiles(t *testing.T) {
	t.Setenv("COLUMNS", "100")
	var out bytes.Buffer
	p := newDownloadProgress(&out, 100, 3)
	p.fileActive("weights/b.bin", true)
	p.fileActive("config.json", true)
	p.render(false)

	got := out.String()
	if !strings.Contains(got, "b.bin,config.json") {
		t.Fatalf("progress does not list active files: %q", got)
	}
	for _, r := range got {
		if r > 127 {
			t.Fatalf("progress contains non-ASCII display glyph %U: %q", r, got)
		}
	}
}

func TestFormatDuration(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		d    time.Duration
		want string
	}{
		{"negative", -time.Second, "--:--"},
		{"seconds", 9 * time.Second, "00:09"},
		{"minutes", 2*time.Minute + 3*time.Second, "02:03"},
		{"hours", 2*time.Hour + 3*time.Minute + 4*time.Second, "02:03:04"},
		{"too long", 100 * time.Hour, "--:--"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatDuration(tt.d); got != tt.want {
				t.Fatalf("formatDuration(%s) = %q, want %q", tt.d, got, tt.want)
			}
		})
	}
}

func TestProgressAccountingIsBounded(t *testing.T) {
	t.Parallel()
	p := newDownloadProgress(&bytes.Buffer{}, 10, 1)
	f := p.file(10)
	f.set(7)
	f.read(20)
	f.set(-1)
	p.mu.Lock()
	current := p.current
	transferred := p.transferred
	p.mu.Unlock()
	if current != 0 || transferred != 20 {
		t.Fatalf("current = %d, transferred = %d", current, transferred)
	}
}

func TestTerminalProgressDoesNotPrintCompletedFiles(t *testing.T) {
	var out bytes.Buffer
	p := newDownloadProgress(&out, 1, 1)
	p.terminal = true
	p.logDone("Done file.bin\n")
	if out.Len() != 0 {
		t.Fatalf("terminal completion created a permanent line: %q", out.String())
	}
}

func TestProgressFitsTerminalWidth(t *testing.T) {
	t.Setenv("COLUMNS", "60")
	var out bytes.Buffer
	p := newDownloadProgress(&out, 100, 3)
	p.fileActive("a/very-long-model-file-name.safetensors", true)
	p.render(false)
	line := strings.TrimPrefix(out.String(), "\r\x1b[2K")
	if len([]rune(line)) > 59 {
		t.Fatalf("progress line has width %d, want at most 59: %q", len([]rune(line)), line)
	}
}

func TestTerminalWidthFallback(t *testing.T) {
	t.Setenv("COLUMNS", "invalid")
	if got := terminalWidth(); got != 79 {
		t.Fatalf("terminalWidth() = %d", got)
	}
}
