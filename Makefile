APP_NAME := tidbcloud-insight

GIT_HASH := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_DIRTY := $(shell git diff --quiet 2>/dev/null || echo "dirty")
DIRTY_HASH := $(if $(GIT_DIRTY),$(shell git diff 2>/dev/null | git hash-object --stdin 2>/dev/null | cut -c1-8),)

VERSION := $(GIT_HASH)$(if $(GIT_DIRTY),-dirty)
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')

LDFLAGS := -ldflags "-s -w \
	-X main.GitHash=$(GIT_HASH) \
	-X main.GitDirty=$(GIT_DIRTY) \
	-X main.DirtyHash=$(DIRTY_HASH) \
	-X main.BuildTime=$(BUILD_TIME)"

.PHONY: all build clean test lint fmt vet install run

all: build

build:
	go build $(LDFLAGS) -o $(APP_NAME) ./cmd/$(APP_NAME)

install:
	go install $(LDFLAGS) ./cmd/$(APP_NAME)

run:
	go run ./cmd/$(APP_NAME)

test:
	go test -v ./...

lint:
	golangci-lint run

fmt:
	go fmt ./...

vet:
	go vet ./...

clean:
	rm -f $(APP_NAME)
	rm -rf metrics.cache/

tidy:
	go mod tidy
