FROM i386/debian:9

RUN apt-get update && apt-get -y install \
    git \
    cmake \
    make \
    libssl-dev \
    python3 \
    python3-dev \
    clang-7 \
    libc++-7-dev \
    libc++abi-7-dev \
  && rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/cc cc /usr/bin/clang-7 100
RUN update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-7 100

ENV CXXFLAGS=-stdlib=libc++
