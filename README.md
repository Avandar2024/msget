# msget

A ModelScope model downloader using only the Go standard library. It builds as a single executable and does not require Python, Git, or the ModelScope SDK.

## Build

Go 1.22 or newer is required:

```bash
go build -o msget .
```

Cross-compilation is also supported:

```bash
GOOS=linux GOARCH=amd64 go build -o msget-linux-amd64 .
```

## Usage

```bash
# Download the full model into ./Qwen3-0.6B
./msget Qwen/Qwen3-0.6B

# Choose an output directory and revision
./msget -o ./model -revision v1.0 Qwen/Qwen3-0.6B

# Download configuration files only; include/exclude may be repeated
./msget -include '*.json' -include '*.jinja' Qwen/Qwen3-0.6B

# Exclude large weight files
./msget -exclude '*.safetensors' Qwen/Qwen3-0.6B
```

For private models, provide the token through an environment variable so it does not remain in shell history:

```bash
MODELSCOPE_API_TOKEN=ms-xxx ./msget owner/private-model
```

Run `./msget -h` for all options. The downloader automatically selects 2-8 connections based on available CPUs. Multiple files are downloaded concurrently, and files of at least 8 MiB may use up to four ranges. Failed transfers are retried five times and server-provided SHA-256 hashes are verified. Connections time out only when connecting or after 60 seconds without data; large files have no total time limit.

Incomplete files and atomically updated range state are stored as `.part` and `.part.meta`. Run the same command again after an interruption to resume. Resume state is tied to the model, revision, path, size, and SHA-256 value, so stale data is discarded when the remote file changes. Sequential resume uses ETag or Last-Modified with `If-Range`. If a server does not support HTTP ranges, the downloader falls back to one connection.

Interactive terminals show an ASCII progress bar, percentage, transferred/total bytes, speed, ETA, file count, connection count, and the names of files currently downloading. Existing partial data is included in total progress. Redirected output disables animation and ANSI colors while preserving one-line completion messages.

The HTTP pool reuses connections while limiting total and per-host concurrency. Idle connections are retained for 90 seconds and closed when a download finishes.

To use the international endpoint or a mirror:

```bash
MODELSCOPE_ENDPOINT=https://www.modelscope.ai ./msget Qwen/Qwen3-0.6B
```
