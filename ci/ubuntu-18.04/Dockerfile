FROM ubuntu:18.04

RUN apt-get update && apt-get -y install \
    git \
    cmake \
    make \
    clang-8 \
    libc++-8-dev \
    libc++abi-8-dev \
    libssl-dev \
    python3 \
    python3-dev \
  && rm -rf /var/lib/apt/lists/*

ENV CC=/usr/bin/clang-8
ENV CXX=/usr/bin/clang++-8
ENV CXXFLAGS=-stdlib=libc++
