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

常用参数可用 `./msget -h` 查看。默认同时下载最多 8 个文件、失败重试 5 次、校验服务端提供的 SHA-256。连接或连续 60 秒收不到数据才会超时，不限制大文件的总下载时长。未完成的文件保存为 `.part`，再次执行相同命令会继续下载。已有且校验通过的文件会跳过。

如果使用国际站或镜像：

```bash
./msget -endpoint https://modelscope.ai owner/model
# 或 export MODELSCOPE_ENDPOINT=https://modelscope.ai
```
