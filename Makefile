APP := msget
CMD := ./cmd/msget
BIN_DIR := bin
VERSION ?= dev
CGO_ENABLED ?= 0
UPX_BIN ?= upx
UPX_FLAGS ?= --best --lzma
BUILD_TAGS ?= nethttpomithttp2
GCFLAGS := all=-l
LDFLAGS := -s -w -buildid= -X main.version=$(VERSION)
BUILD_FLAGS := -tags "$(BUILD_TAGS)" -trimpath -buildvcs=false -gcflags "$(GCFLAGS)" -ldflags "$(LDFLAGS)"

.PHONY: all build build-compressed build-linux-amd64 test check coverage benchmark clean

BENCH_TIME ?= 1x
BENCH_COUNT ?= 5

all: check build

build:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_FLAGS) -o $(BIN_DIR)/$(APP) $(CMD)

build-compressed: build
	cp $(BIN_DIR)/$(APP) $(BIN_DIR)/$(APP)-compressed
	$(UPX_BIN) $(UPX_FLAGS) $(BIN_DIR)/$(APP)-compressed
	$(UPX_BIN) -t $(BIN_DIR)/$(APP)-compressed

build-linux-amd64:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=$(CGO_ENABLED) GOOS=linux GOARCH=amd64 go build $(BUILD_FLAGS) -o $(BIN_DIR)/$(APP)-linux-amd64 $(CMD)

test:
	go test ./...

check:
	test -z "$$(gofmt -l .)"
	go vet ./...
	go test -race ./...

coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out
	@go tool cover -func=coverage.out | awk '/^total:/ { gsub(/%/, "", $$3); if ($$3 < 75) { print "coverage " $$3 "% is below 75%"; exit 1 } }'

benchmark:
	go test ./internal/downloader -run '^$$' -bench '^BenchmarkParallelDownload$$' -benchmem -benchtime=$(BENCH_TIME) -count=$(BENCH_COUNT)

clean:
	rm -rf $(BIN_DIR) coverage.out
