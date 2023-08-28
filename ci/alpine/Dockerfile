FROM alpine:latest

# A version field to invalidate Cirrus's build cache when needed, as suggested in
# https://github.com/cirruslabs/cirrus-ci-docs/issues/544#issuecomment-566066822
ENV DOCKERFILE_VERSION 20230823

RUN apk add --no-cache \
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

RUN pip3 install websockets junit2html
