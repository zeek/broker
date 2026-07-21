#!/bin/sh
set -e
set -x

apk add --no-cache \
  bash \
  cmake \
  curl \
  diffutils \
  flex-dev \
  g++ \
  git \
  linux-headers \
  make \
  openssl-dev \
  py3-pip \
  python3 \
  python3-dev

pip3 install --break-system-packages junit2html
