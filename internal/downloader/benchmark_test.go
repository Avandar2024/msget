package downloader

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const benchmarkDownloadSize int64 = 256 << 20

type zeroReaderAt struct{}

func (zeroReaderAt) ReadAt(p []byte, _ int64) (int, error) {
	clear(p)
	return len(p), nil
}

func BenchmarkParallelDownload(b *testing.B) {
	client := &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		bounds := strings.Split(strings.TrimPrefix(req.Header.Get("Range"), "bytes="), "-")
		if len(bounds) != 2 {
			return nil, fmt.Errorf("invalid range %q", req.Header.Get("Range"))
		}
		start, err := strconv.ParseInt(bounds[0], 10, 64)
		if err != nil {
			return nil, err
		}
		end, err := strconv.ParseInt(bounds[1], 10, 64)
		if err != nil {
			return nil, err
		}
		header := make(http.Header)
		header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, benchmarkDownloadSize))
		return &http.Response{
			StatusCode: http.StatusPartialContent,
			Header:     header,
			Body:       io.NopCloser(io.NewSectionReader(zeroReaderAt{}, start, end-start+1)),
			Request:    req,
		}, nil
	})}
	d := Downloader{Endpoint: "https://benchmark.invalid", Workers: 4, Parts: 4, RangeSize: 16 << 20, Timeout: time.Minute, Client: client}
	dir := b.TempDir()
	part := filepath.Join(dir, "model.part")
	file := repoFile{Path: "model", Size: benchmarkDownloadSize}
	b.SetBytes(benchmarkDownloadSize)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := os.Remove(part); err != nil && !os.IsNotExist(err) {
			b.Fatal(err)
		}
		if err := os.Remove(part + ".meta"); err != nil && !os.IsNotExist(err) {
			b.Fatal(err)
		}
		progress := newDownloadProgress(io.Discard, benchmarkDownloadSize, 1)
		if err := d.downloadParallel(context.Background(), "bench/model", "main", part, file, make(chan struct{}, 4), progress.file(benchmarkDownloadSize), progress); err != nil {
			b.Fatal(err)
		}
	}
}
