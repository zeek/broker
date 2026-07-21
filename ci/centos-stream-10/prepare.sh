#!/bin/sh
set -e
set -x

dnf -y install \
    cmake \
    diffutils \
    gcc \
    gcc-c++ \
    git \
    make \
    openssl \
    openssl-devel \
    python3 \
    python3-devel

dnf clean all
rm -rf /var/cache/dnf
