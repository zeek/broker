FROM ubuntu:18.04

ENV CMAKE_DIR "/opt/cmake"
ENV CMAKE_VERSION "3.19.1"
ENV PATH "${CMAKE_DIR}/bin:${PATH}"

RUN apt update -y \
    && apt install -y \
       curl \
       diffutils \
       g++-8 \
       gcc-8 \
       git \
       libssl-dev \
       make \
       python3 \
       python3-dev \
    && apt-get autoclean \
    && rm -rf /var/lib/apt/lists/* \
    # Recent CMake.
    && mkdir -p "${CMAKE_DIR}" \
    && curl -sSL "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-Linux-x86_64.tar.gz" | tar xzf - -C "${CMAKE_DIR}" --strip-components 1

ENV CC=/usr/bin/gcc-8
ENV CXX=/usr/bin/g++-8
