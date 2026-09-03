package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	NetworkAuto = "auto"
	NetworkIPv4 = "ipv4"
	NetworkIPv6 = "ipv6"
	NetworkDual = "dual"
)

func (d *Downloader) client() *http.Client {
	if d.Client != nil {
		return d.Client
	}
	workers := max(1, d.Workers)
	idleTimeout := d.IdleConnTimeout
	if idleTimeout <= 0 {
		idleTimeout = 90 * time.Second
	}
	connectTimeout := d.Timeout
	if connectTimeout <= 0 || connectTimeout > 30*time.Second {
		connectTimeout = 30 * time.Second
	}
	newTransport := func(network string) *http.Transport {
		dialer := &net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}
		return &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: func(ctx context.Context, _, address string) (net.Conn, error) {
				return dialer.DialContext(ctx, network, address)
			},
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          max(16, workers*2),
			MaxIdleConnsPerHost:   workers,
			MaxConnsPerHost:       workers,
			IdleConnTimeout:       idleTimeout,
			TLSHandshakeTimeout:   min(10*time.Second, connectTimeout),
			ResponseHeaderTimeout: d.Timeout,
			ExpectContinueTimeout: time.Second,
		}
	}
	var transport http.RoundTripper
	switch d.Network {
	case NetworkIPv4:
		transport = newTransport("tcp4")
	case NetworkIPv6:
		transport = newTransport("tcp6")
	case NetworkDual:
		transport = &dualTransport{transports: [2]http.RoundTripper{newTransport("tcp4"), newTransport("tcp6")}}
	case "":
		// Dual is the library default as well as the CLI default.
		transport = &dualTransport{transports: [2]http.RoundTripper{newTransport("tcp4"), newTransport("tcp6")}}
	default:
		transport = newTransport("tcp")
	}
	return &http.Client{Transport: transport}
}

// dualTransport keeps separate IPv4 and IPv6 connection pools. Requests are
// weighted by each family's measured response throughput, with occasional
// probes and connection-error fallback for incomplete dual-stack hosts.
type dualTransport struct {
	transports [2]http.RoundTripper
	mu         sync.Mutex
	next       uint64
	rate       [2]float64
	samples    [2]uint64
	failures   [2]uint64
}

func (t *dualTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	first := t.choose()
	resp, err := t.transports[first].RoundTrip(req)
	if err == nil {
		resp.Body = &measuredBody{ReadCloser: resp.Body, started: time.Now(), family: first, owner: t}
		return resp, nil
	}
	t.recordFailure(first)
	if req.Body != nil && req.GetBody == nil {
		return nil, err
	}
	retry := req.Clone(req.Context())
	if req.Body != nil {
		retry.Body, _ = req.GetBody()
	}
	resp, retryErr := t.transports[1-first].RoundTrip(retry)
	if retryErr != nil {
		t.recordFailure(1 - first)
		return nil, errors.Join(err, retryErr)
	}
	resp.Body = &measuredBody{ReadCloser: resp.Body, started: time.Now(), family: 1 - first, owner: t}
	return resp, nil
}

func (t *dualTransport) choose() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	ticket := t.next
	t.next++
	if t.samples[0] == 0 && t.samples[1] == 0 {
		return int(ticket % 2)
	}
	// Give an unmeasured family occasional probes, while preferring the
	// measured family for the other requests.
	if t.samples[0] == 0 {
		if ticket%8 == 0 {
			return 0
		}
		return 1
	}
	if t.samples[1] == 0 {
		if ticket%8 == 0 {
			return 1
		}
		return 0
	}
	if t.failures[0] >= 3 && ticket%16 != 0 {
		return 1
	}
	if t.failures[1] >= 3 && ticket%16 != 0 {
		return 0
	}
	// A deterministic weighted choice avoids adding a random source to the
	// downloader. Rates are EWMA values in bytes/sec.
	weight := int(t.rate[0] * 1000 / (t.rate[0] + t.rate[1]))
	if weight < 1 {
		weight = 1
	}
	if weight > 999 {
		weight = 999
	}
	if int(ticket%1000) < weight {
		return 0
	}
	return 1
}

func (t *dualTransport) recordFailure(family int) {
	t.mu.Lock()
	t.failures[family]++
	t.mu.Unlock()
}

func (t *dualTransport) recordRate(family int, bytes int64, elapsed time.Duration) {
	if bytes <= 0 || elapsed <= 0 {
		return
	}
	rate := float64(bytes) / elapsed.Seconds()
	t.mu.Lock()
	if t.samples[family] == 0 {
		t.rate[family] = rate
	} else {
		t.rate[family] = t.rate[family]*0.7 + rate*0.3
	}
	t.samples[family]++
	t.failures[family] = 0
	t.mu.Unlock()
}

type measuredBody struct {
	io.ReadCloser
	started time.Time
	family  int
	owner   *dualTransport
	bytes   int64
	once    sync.Once
}

func (b *measuredBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.bytes += int64(n)
	if err == io.EOF {
		b.Close()
	}
	return n, err
}

func (b *measuredBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(func() { b.owner.recordRate(b.family, b.bytes, time.Since(b.started)) })
	return err
}

func (t *dualTransport) CloseIdleConnections() {
	for _, transport := range t.transports {
		if closer, ok := transport.(interface{ CloseIdleConnections() }); ok {
			closer.CloseIdleConnections()
		}
	}
}

func (d *Downloader) writer() io.Writer {
	if d.Out != nil {
		return d.Out
	}
	return io.Discard
}

func (d *Downloader) headers(req *http.Request) {
	userAgent := d.UserAgent
	if userAgent == "" {
		userAgent = "msget/dev"
	}
	req.Header.Set("User-Agent", userAgent)
	if d.Token != "" {
		req.Header.Set("Authorization", "Bearer "+d.Token)
		req.Header.Set("X-ModelScope-Token", d.Token)
	}
}

func responseError(action string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	message := strings.TrimSpace(string(body))
	if message == "" {
		message = resp.Status
	}
	return fmt.Errorf("%s: HTTP %d: %s", action, resp.StatusCode, message)
}

func validateRepo(repo string) error {
	parts := strings.Split(repo, "/")
	if len(parts) < 2 {
		return errors.New("model ID must be namespace/model, for example Qwen/Qwen3-0.6B")
	}
	for _, p := range parts {
		if p == "" || p == "." || p == ".." {
			return errors.New("invalid model ID")
		}
	}
	return nil
}

func escapeRepo(repo string) string {
	parts := strings.Split(repo, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	return strings.Join(parts, "/")
}
