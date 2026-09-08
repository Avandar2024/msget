package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/jiyuren/msget/internal/downloader"
)

var version = "dev"

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error {
	if v != "" {
		*s = append(*s, v)
	}
	return nil
}

func usage() {
	fmt.Fprintf(flag.CommandLine.Output(), `msget - ModelScope and Hugging Face mirror model downloader

Usage:
  msget [options] <namespace/model>

Examples:
  msget Qwen/Qwen3-0.6B
  msget -source hf Qwen/Qwen3-0.6B
  msget -o ./model -include '*.json' Qwen/Qwen3-0.6B
  MODELSCOPE_API_TOKEN=ms-xxx msget owner/private-model

Options:
`)
	flag.PrintDefaults()
}

func main() {
	var includes, excludes stringList
	output := flag.String("o", "", "output directory (default: model name)")
	source := flag.String("source", downloader.SourceAuto, "model source: auto, modelscope, or hf (Hugging Face mirror)")
	revision := flag.String("revision", "", "branch, tag, or commit (default: master for ModelScope, main for hf)")
	network := flag.String("network", downloader.NetworkDual, "connection family: auto, ipv4, ipv6, or dual")
	connections := flag.Int("connections", 0, "maximum concurrent downloads (default: 2-8 based on CPUs)")

	showVersion := flag.Bool("version", false, "show version")
	flag.Var(&includes, "include", "download only files matching this glob (repeatable)")
	flag.Var(&excludes, "exclude", "exclude files matching this glob (repeatable)")
	flag.Usage = usage
	flag.Parse()

	if *showVersion {
		fmt.Println("msget", version)
		return
	}
	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}
	repo := flag.Arg(0)
	switch *network {
	case downloader.NetworkAuto, downloader.NetworkIPv4, downloader.NetworkIPv6, downloader.NetworkDual:
	default:
		fatal(fmt.Errorf("invalid -network %q (want auto, ipv4, ipv6, or dual)", *network))
	}
	if *output == "" {
		parts := strings.Split(strings.Trim(repo, "/"), "/")
		*output = parts[len(parts)-1]
	}
	if *connections < 0 || *connections > 64 {
		fatal(fmt.Errorf("invalid -connections %d (want 1-64, or 0 for automatic)", *connections))
	}

	endpoint, token := envOr("MODELSCOPE_ENDPOINT", "https://modelscope.cn"), os.Getenv("MODELSCOPE_API_TOKEN")
	hfEndpoint, hfToken := envOr("HF_ENDPOINT", "https://hf-mirror.com"), os.Getenv("HF_TOKEN")
	switch *source {
	case downloader.SourceAuto, downloader.SourceModelScope:
	case downloader.SourceHF:
		endpoint, token = hfEndpoint, hfToken
	default:
		fatal(fmt.Errorf("invalid -source %q (want auto, modelscope, or hf)", *source))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	workers := min(8, max(2, runtime.NumCPU()))
	parts := workers
	if *connections > 0 {
		workers = *connections
		parts = workers
	}
	d := downloader.Downloader{
		Source:          *source,
		Endpoint:        strings.TrimRight(endpoint, "/"),
		Token:           token,
		HFEndpoint:      strings.TrimRight(hfEndpoint, "/"),
		HFToken:         hfToken,
		UserAgent:       "msget/" + version,
		Workers:         workers,
		Parts:           parts,
		RangeSize:       64 << 20,
		Retries:         5,
		Timeout:         60 * time.Second,
		IdleConnTimeout: 90 * time.Second,
		Network:         *network,
		Verify:          true,
		Out:             os.Stderr,
	}
	if err := d.Download(ctx, repo, *revision, *output, includes, excludes); err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Fprintln(os.Stderr, "Download paused. Run the same command to resume.")
			os.Exit(130)
		}
		fatal(err)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "Error:", err)
	os.Exit(1)
}
