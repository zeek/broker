#! /usr/bin/env bash

set -e
set -x

# When not running in Cirrus, pick system defaults to allow developers to run
# this script locally to run clang-tidy.
if [ -z "${BROKER_CI_CPUS}" ]; then
  if [ `uname` = "Linux" ]; then
    BROKER_CI_CPUS=`nproc --all`
  elif [ `uname` = "Darwin" ]; then
    BROKER_CI_CPUS=`sysctl -n hw.physicalcpu`
  else
    echo "Sorry, OS not detected. Please set BROKER_CI_CPUS manually."
    exit 1
  fi
fi

CXX=clang++ ./configure --build-type=debug
clang-tidy --version
run-clang-tidy -p build -j ${BROKER_CI_CPUS} $PWD/src
