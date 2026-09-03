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
	workers := flag.Int("workers", min(8, max(2, runtime.NumCPU())), "最大并发下载连接数")
	parts := flag.Int("parts", 4, "大文件并行分片数（总连接数仍受 workers 限制）")
	retries := flag.Int("retries", 5, "每个文件的重试次数")
	timeout := flag.Duration("timeout", 60*time.Second, "连接或连续无数据超时")
	endpoint := flag.String("endpoint", envOr("MODELSCOPE_ENDPOINT", "https://modelscope.cn"), "ModelScope 服务地址")
	token := flag.String("token", os.Getenv("MODELSCOPE_API_TOKEN"), "访问令牌（也可用 MODELSCOPE_API_TOKEN）")
	verify := flag.Bool("verify", true, "校验文件 SHA-256")
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
	if *workers < 1 || *parts < 1 || *retries < 0 || *timeout <= 0 {
		fatal(errors.New("workers 和 parts 必须大于 0，retries 不能为负，timeout 必须大于 0"))
	}

	repo := flag.Arg(0)
	if *output == "" {
		parts := strings.Split(strings.Trim(repo, "/"), "/")
		*output = parts[len(parts)-1]
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	d := Downloader{
		Endpoint: strings.TrimRight(*endpoint, "/"), Token: *token,
		Workers: *workers, Parts: *parts, Retries: *retries, Timeout: *timeout, Verify: *verify,
		Out: os.Stderr,
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
