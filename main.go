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
	fmt.Fprintf(flag.CommandLine.Output(), `msget - ModelScope model downloader

Usage:
  msget [options] <namespace/model>

Examples:
  msget Qwen/Qwen3-0.6B
  msget -o ./model -include '*.json' Qwen/Qwen3-0.6B
  MODELSCOPE_API_TOKEN=ms-xxx msget owner/private-model

Options:
`)
	flag.PrintDefaults()
}

func main() {
	var includes, excludes stringList
	output := flag.String("o", "", "output directory (default: model name)")
	revision := flag.String("revision", "master", "branch, tag, or commit")
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
	if *output == "" {
		parts := strings.Split(strings.Trim(repo, "/"), "/")
		*output = parts[len(parts)-1]
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	workers := min(8, max(2, runtime.NumCPU()))
	d := Downloader{
		Endpoint:        strings.TrimRight(envOr("MODELSCOPE_ENDPOINT", "https://modelscope.cn"), "/"),
		Token:           os.Getenv("MODELSCOPE_API_TOKEN"),
		Workers:         workers,
		Parts:           min(4, workers),
		Retries:         5,
		Timeout:         60 * time.Second,
		IdleConnTimeout: 90 * time.Second,
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
