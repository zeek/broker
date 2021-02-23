FROM debian:10

RUN apt update -y \
    && apt install -y \
       cmake \
       g++ \
       gcc \
       git \
       libssl-dev \
       make \
       python3 \
       python3-dev \
    && apt autoclean \
    && rm -rf /var/lib/apt/lists/*
