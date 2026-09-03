# msget

一个只依赖 Go 标准库的 ModelScope 模型下载器。编译后是单个可执行文件，不需要 Python、Git 或 ModelScope SDK。

## 构建

需要 Go 1.22 或更高版本：

```bash
go build -trimpath -ldflags="-s -w" -o msget .
```

也可以交叉编译：

```bash
GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o msget-linux-amd64 .
```

## 使用

```bash
# 下载整个模型到 ./Qwen3-0.6B
./msget Qwen/Qwen3-0.6B

# 指定目录和 revision
./msget -o ./model -revision master Qwen/Qwen3-0.6B

# 只下载配置文件；include/exclude 可以重复
./msget -include '*.json' -include '**/*.json' Qwen/Qwen3-0.6B

# 排除大权重
./msget -exclude '*.safetensors' -exclude '**/*.safetensors' Qwen/Qwen3-0.6B
```

私有模型通过环境变量提供 token，避免 token 留在 shell 历史中：

```bash
export MODELSCOPE_API_TOKEN='ms-...'
./msget owner/private-model
```

可用参数仅保留输出目录、revision、文件筛选和版本信息，运行 `./msget -h` 即可查看。下载器会根据 CPU 自动选择 2–8 条连接；多个文件会并发下载，8 MiB 以上的大文件最多拆成 4 个分片。失败自动重试 5 次，并校验服务端提供的 SHA-256。连接或连续 60 秒收不到数据才会超时，不限制大文件的总下载时长。

未完成的文件和原子更新的分片状态保存为 `.part` / `.part.meta`，再次执行相同命令只会补齐缺失分片。续传状态绑定模型、revision、文件路径、大小和 SHA-256；远端文件身份变化时会放弃旧进度，避免把不同版本拼接在一起。顺序续传会使用服务器的 ETag 或 Last-Modified 发送 `If-Range`。若服务器不支持 HTTP Range，会自动退回单连接下载。

在交互式终端中会显示彩色动态进度条、完成百分比、已下载/总大小、传输速度、预计剩余时间、文件数和当前连接数。已有的断点数据会直接计入总进度；重定向输出到日志或管道时会自动关闭动画和 ANSI 颜色，保留逐行完成信息。

HTTP 连接池会复用已有连接，并自动限制总下载连接数和单主机连接数。空闲连接保留 90 秒后回收；一次下载结束后会立即关闭下载器自行创建的空闲连接。

如果使用国际站或镜像：

```bash
MODELSCOPE_ENDPOINT=https://modelscope.ai ./msget owner/model
```
