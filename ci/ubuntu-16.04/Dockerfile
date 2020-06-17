FROM ubuntu:16.04

RUN apt-get update && apt-get -y install \
    git \
    cmake \
    make \
    gcc \
    g++ \
    libssl-dev \
    python3 \
    python3-dev \
    bsdmainutils \
    wget \
    xz-utils \
  && rm -rf /var/lib/apt/lists/*

RUN wget -q https://releases.llvm.org/9.0.0/clang+llvm-9.0.0-x86_64-linux-gnu-ubuntu-16.04.tar.xz
RUN mkdir /clang-9
RUN tar --strip-components=1 -C /clang-9 -xvf clang+llvm-9.0.0-x86_64-linux-gnu-ubuntu-16.04.tar.xz
RUN update-alternatives --install /usr/bin/cc cc /clang-9/bin/clang 100
RUN update-alternatives --install /usr/bin/c++ c++ /clang-9/bin/clang++ 100

ENV CXXFLAGS=-stdlib=libc++
ENV LD_LIBRARY_PATH=/clang-9/lib
