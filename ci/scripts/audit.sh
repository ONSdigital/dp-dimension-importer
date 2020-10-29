#!/bin/bash -eux

export cwd=$(pwd)

pushd $cwd/dp-dimension-importer
  make audit
popd