#!/bin/sh
set -e
set -x

export DEBIAN_FRONTEND=noninteractive
export TZ=America/Los_Angeles

apt-get update && apt-get -y install \
    cmake \
    g++ \
    git \
    libssl-dev \
    make \
    python3 \
    python3-dev

apt autoclean
rm -rf /var/lib/apt/lists/*
