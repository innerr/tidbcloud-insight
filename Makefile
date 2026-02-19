APP_NAME := tidbcloud-insight
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS := -ldflags "-s -w -X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)"

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
