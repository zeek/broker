#!/bin/sh
set -e
set -x

export DEBIAN_FRONTEND=noninteractive
export TZ=America/Los_Angeles

apt-get update && apt-get -y install \
    clang \
    clang-tidy \
    cmake \
    curl \
    g++ \
    gcc \
    git \
    libssl-dev \
    make \
    python3 \
    python3-dev \
    python3-pip \
    python3-venv

apt autoclean
rm -rf /var/lib/apt/lists/*
