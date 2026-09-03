APP := msget
CMD := ./cmd/msget
BIN_DIR := bin
VERSION ?= dev
CGO_ENABLED ?= 0
GCFLAGS := all=-l
LDFLAGS := -s -w -buildid= -X main.version=$(VERSION)
BUILD_FLAGS := -trimpath -buildvcs=false -gcflags "$(GCFLAGS)" -ldflags "$(LDFLAGS)"

.PHONY: all build build-linux-amd64 test check coverage clean

all: check build

build:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_FLAGS) -o $(BIN_DIR)/$(APP) $(CMD)

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

clean:
	rm -rf $(BIN_DIR) coverage.out
