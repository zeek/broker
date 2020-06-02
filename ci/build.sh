#! /usr/bin/env bash

set -e
set -x

./configure ${BROKER_CI_CONFIGURE_FLAGS}
cd build
make -j ${BROKER_CI_CPUS}
