FROM ubuntu:22.10

ENV DEBIAN_FRONTEND="noninteractive" TZ="America/Los_Angeles"

# A version field to invalidate Cirrus's build cache when needed, as suggested in
# https://github.com/cirruslabs/cirrus-ci-docs/issues/544#issuecomment-566066822
ENV DOCKERFILE_VERSION 20230813

RUN apt-get update && apt-get -y install \
    cmake \
    g++ \
    git \
    libssl-dev \
    make \
    python3 \
    python3-dev \
  && apt autoclean \
  && rm -rf /var/lib/apt/lists/*
