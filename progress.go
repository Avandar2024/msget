package main

import (
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"time"
)

type downloadProgress struct {
	mu          sync.Mutex
	out         io.Writer
	total       int64
	current     int64
	transferred int64
	files       int
	doneFiles   int
	connections int
	started     time.Time
	terminal    bool
	stop        chan struct{}
	done        chan struct{}
	finishOnce  sync.Once
	frame       int
}

type fileProgress struct {
	mu      sync.Mutex
	parent  *downloadProgress
	size    int64
	current int64
}

func newDownloadProgress(out io.Writer, total int64, files int) *downloadProgress {
	p := &downloadProgress{out: out, total: total, files: files, started: time.Now(), stop: make(chan struct{}), done: make(chan struct{})}
	if f, ok := out.(*os.File); ok {
		if st, err := f.Stat(); err == nil {
			p.terminal = st.Mode()&os.ModeCharDevice != 0
		}
	}
	if !p.terminal {
		close(p.done)
		return p
	}
	go func() {
		defer close(p.done)
		ticker := time.NewTicker(120 * time.Millisecond)
		defer ticker.Stop()
		p.render(false)
		for {
			select {
			case <-ticker.C:
				p.render(false)
			case <-p.stop:
				p.render(true)
				return
			}
		}
	}()
	return p
}

func (p *downloadProgress) file(size int64) *fileProgress {
	return &fileProgress{parent: p, size: size}
}

func (f *fileProgress) set(n int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n = max(int64(0), min(n, f.size))
	f.parent.addCurrent(n - f.current)
	f.current = n
}

func (f *fileProgress) read(n int) {
	if n <= 0 {
		return
	}
	f.mu.Lock()
	delta := min(int64(n), f.size-f.current)
	f.current += delta
	f.mu.Unlock()
	f.parent.addRead(delta, int64(n))
}

func (p *downloadProgress) addCurrent(delta int64) {
	p.mu.Lock()
	p.current = max(int64(0), min(p.total, p.current+delta))
	p.mu.Unlock()
}

func (p *downloadProgress) addRead(current, transferred int64) {
	p.mu.Lock()
	p.current = max(int64(0), min(p.total, p.current+current))
	p.transferred += transferred
	p.mu.Unlock()
}

func (p *downloadProgress) fileDone() {
	p.mu.Lock()
	p.doneFiles++
	p.mu.Unlock()
}

func (p *downloadProgress) connection(delta int) {
	p.mu.Lock()
	p.connections += delta
	p.mu.Unlock()
}

func (p *downloadProgress) finish() {
	p.finishOnce.Do(func() {
		if p.terminal {
			close(p.stop)
		}
		<-p.done
	})
}

func (p *downloadProgress) log(format string, args ...any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.terminal {
		fmt.Fprint(p.out, "\r\x1b[2K")
	}
	fmt.Fprintf(p.out, format, args...)
}

func (p *downloadProgress) render(final bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	elapsed := time.Since(p.started).Seconds()
	var speed float64
	if elapsed > 0 {
		speed = float64(p.transferred) / elapsed
	}
	percent := 1.0
	if p.total > 0 {
		percent = float64(p.current) / float64(p.total)
	}
	percent = math.Max(0, math.Min(1, percent))
	const width = 28
	filled := int(percent * width)
	bar := strings.Repeat("━", filled) + strings.Repeat("─", width-filled)
	eta := "--:--"
	if speed > 0 && p.current < p.total {
		eta = formatDuration(time.Duration(float64(p.total-p.current)/speed) * time.Second)
	} else if p.current >= p.total {
		eta = "00:00"
	}
	spinner := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}[p.frame%10]
	p.frame++
	if final {
		if p.current >= p.total && p.doneFiles == p.files {
			spinner = "✓"
		} else {
			spinner = "!"
		}
	}
	fmt.Fprintf(p.out, "\r\x1b[2K\x1b[36m%s\x1b[0m \x1b[32m%s\x1b[0m %6.2f%%  %s/%s  %s/s  ETA %s  文件 %d/%d  连接 %d", spinner, bar, percent*100, humanBytes(p.current), humanBytes(p.total), humanBytes(int64(speed)), eta, p.doneFiles, p.files, p.connections)
	if final {
		fmt.Fprintln(p.out)
	}
}

func formatDuration(d time.Duration) string {
	if d < 0 || d > 99*time.Hour {
		return "--:--"
	}
	total := int(d.Round(time.Second).Seconds())
	if total >= 3600 {
		return fmt.Sprintf("%02d:%02d:%02d", total/3600, total/60%60, total%60)
	}
	return fmt.Sprintf("%02d:%02d", total/60, total%60)
}
