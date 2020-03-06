#!/bin/bash -eux

cwd=$(pwd)

pushd $cwd/dp-dimension-importer
  make build && mv build/$(go env GOOS)-$(go env GOARCH)/* $cwd/build
  cp Dockerfile.concourse $cwd/build
popd
