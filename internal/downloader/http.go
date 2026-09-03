package downloader

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
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
	return &http.Client{Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          max(16, workers*2),
		MaxIdleConnsPerHost:   workers,
		MaxConnsPerHost:       workers,
		IdleConnTimeout:       idleTimeout,
		TLSHandshakeTimeout:   min(10*time.Second, connectTimeout),
		ResponseHeaderTimeout: d.Timeout,
		ExpectContinueTimeout: time.Second,
	}}
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
