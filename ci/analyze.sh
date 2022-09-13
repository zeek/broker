#! /usr/bin/env bash

set -e
set -x

CXX=clang++ ./configure --build-type=debug

run-clang-tidy -p build -j ${BROKER_CI_CPUS} $PWD/src
