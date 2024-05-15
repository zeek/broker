FROM debian:11

ENV DEBIAN_FRONTEND="noninteractive" TZ="America/Los_Angeles"

# A version field to invalide Cirrus's build cache when needed, as suggested in
# https://github.com/cirruslabs/cirrus-ci-docs/issues/544#issuecomment-566066822
ENV DOCKERFILE_VERSION 20220519

RUN apt-get update && apt-get -y install \
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
    python3-venv \
  && apt autoclean \
  && rm -rf /var/lib/apt/lists/*
