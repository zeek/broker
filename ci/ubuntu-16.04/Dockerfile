FROM ubuntu:16.04

RUN apt update -y \
    && apt install -y \
       clang-8 \
       cmake \
       git \
       libc++-8-dev \
       libc++abi-8-dev \
       libssl-dev \
       make \
       python3 \
       python3-dev \
    && apt autoclean \
    && rm -rf /var/lib/apt/lists/*

ENV CC=/usr/bin/clang-8
ENV CXX=/usr/bin/clang++-8
ENV CXXFLAGS=-stdlib=libc++
