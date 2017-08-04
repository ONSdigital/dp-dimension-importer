SHELL=bash

BUILD=build
BUILD_ARCH=$(BUILD)/$(GOOS)-$(GOARCH)
BIN_DIR?=.

export GOOS?=$(shell go env GOOS)
export GOARCH?=$(shell go env GOARCH)

build:
    #@mkdir -p $(BUILD_ARCH)/$(BIN_DIR)
    #go build -o $(BUILD_ARCH)/$(BIN_DIR)/dp-dimension-importer cmd//dp-dimension-importer/main.go
	go build -o build/dp-dimension-importer
debug: build
	HUMAN_LOG=1 ./build/dp-dimension-importer -bind-addr=":20000" -import-addr="http://localhost:21000" -log-level=error
	#HUMAN_LOG=1 go run cmd/dp-dimension-importer/main.go
test:
    #go test -cover $(shell go list ./... | grep -v /vendor/)
.PHONY: build debug test