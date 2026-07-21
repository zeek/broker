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
    python313 \
    python313-devel

rm -rf /var/cache/zypp
