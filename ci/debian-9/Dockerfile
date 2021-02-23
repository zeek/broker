FROM debian:9

RUN apt update -y \
    && apt install -y \
       clang-7 \
       cmake \
       git \
       libc++-7-dev \
       libc++abi-7-dev \
       libssl-dev \
       make \
       python3 \
       python3-dev \
    && apt autoclean \
    && rm -rf /var/lib/apt/lists/*

ENV CC=/usr/bin/clang-7
ENV CXX=/usr/bin/clang++-7
ENV CXXFLAGS=-stdlib=libc++
