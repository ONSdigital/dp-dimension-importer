SHELL=bash

BUILD=build
BUILD_ARCH=$(BUILD)/$(GOOS)-$(GOARCH)
BIN_DIR?=.

export GOOS?=$(shell go env GOOS)
export GOARCH?=$(shell go env GOARCH)

BUILD_TIME=$(shell date +%s)
GIT_COMMIT=$(shell git rev-parse HEAD)
VERSION ?= $(shell git tag --points-at HEAD | grep ^v | head -n 1)
LDFLAGS=-ldflags "-w -s -X 'main.Version=${VERSION}' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.GitCommit=$(GIT_COMMIT)'"

export GRAPH_DRIVER_TYPE?=neptune
export GRAPH_ADDR?=ws://localhost:8182/gremlin

build:
	@mkdir -p $(BUILD_ARCH)/$(BIN_DIR)
	go build $(LDFLAGS) -o $(BUILD_ARCH)/$(BIN_DIR)/dp-dimension-importer cmd/dp-dimension-importer/main.go
debug: build
	GRAPH_ADDR=$(GRAPH_ADDR) GRAPH_DRIVER_TYPE=$(GRAPH_DRIVER_TYPE) HUMAN_LOG=1 go run $(LDFLAGS) cmd/dp-dimension-importer/main.go
clean:
	rm
test:
	go test -cover -race $(shell go list ./... | grep -v /vendor/)
.PHONY: build debug test
