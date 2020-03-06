#!/bin/bash -eux

cwd=$(pwd)

pushd $cwd/dp-dimension-importer
  make test
popd