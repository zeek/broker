FROM ubuntu:20.04

RUN apt update -y \
    && DEBIAN_FRONTEND="noninteractive" apt install -y \
       cmake \
       g++ \
       git \
       libssl-dev \
       make \
       python3 \
       python3-dev \
    && apt autoclean \
    && rm -rf /var/lib/apt/lists/*
