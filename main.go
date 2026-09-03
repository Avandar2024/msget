package main

import (
	"context"
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
	fmt.Fprintf(flag.CommandLine.Output(), `msget - ModelScope 模型下载器

用法:
  msget [选项] <namespace/model>

示例:
  msget Qwen/Qwen3-0.6B
  msget -o ./model -include '*.json' Qwen/Qwen3-0.6B
  MODELSCOPE_API_TOKEN=ms-xxx msget owner/private-model

选项:
`)
	flag.PrintDefaults()
}

func main() {
	var includes, excludes stringList
	output := flag.String("o", "", "输出目录（默认取模型名称）")
	revision := flag.String("revision", "master", "分支、标签或提交")
	showVersion := flag.Bool("version", false, "显示版本")
	flag.Var(&includes, "include", "只下载匹配 glob 的文件，可重复")
	flag.Var(&excludes, "exclude", "排除匹配 glob 的文件，可重复")
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
	fmt.Fprintln(os.Stderr, "错误:", err)
	os.Exit(1)
}
