#!/bin/sh
set -e
set -x

zypper in -y \
    cmake \
    gcc \
    gcc-c++ \
    git \
    libopenssl-devel \
    make \
    python3 \
    python3-devel

rm -rf /var/cache/zypp
