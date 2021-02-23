FROM ubuntu:18.04

RUN apt update -y \
    && apt install -y \
       cmake \
       g++-8 \
       gcc-8 \
       git \
       libssl-dev \
       make \
       python3 \
       python3-dev \
    && apt-get autoclean \
    && rm -rf /var/lib/apt/lists/*

ENV CC=/usr/bin/gcc-8
ENV CXX=/usr/bin/g++-8
