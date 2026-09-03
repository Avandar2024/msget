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
	"sync/atomic"
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
// alternated between them, with a connection-error fallback for hosts that are
// not reachable over one of the address families.
type dualTransport struct {
	transports [2]http.RoundTripper
	next       atomic.Uint64
}

func (t *dualTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	first := int((t.next.Add(1) - 1) % 2)
	resp, err := t.transports[first].RoundTrip(req)
	if err == nil {
		return resp, nil
	}
	if req.Body != nil && req.GetBody == nil {
		return nil, err
	}
	retry := req.Clone(req.Context())
	if req.Body != nil {
		retry.Body, _ = req.GetBody()
	}
	resp, retryErr := t.transports[1-first].RoundTrip(retry)
	if retryErr != nil {
		return nil, errors.Join(err, retryErr)
	}
	return resp, nil
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
