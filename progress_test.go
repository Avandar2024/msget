package main

import (
	"bytes"
	"strings"
	"testing"
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
