#!/bin/bash -eux

export cwd=$(pwd)

pushd $cwd/dp-code-list-api
  make audit
popd