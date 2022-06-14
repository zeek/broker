FROM ubuntu:18.04

ENV DEBIAN_FRONTEND="noninteractive" TZ="America/Los_Angeles"

# A version field to invalide Cirrus's build cache when needed, as suggested in
# https://github.com/cirruslabs/cirrus-ci-docs/issues/544#issuecomment-566066822
ENV DOCKERFILE_VERSION 20220519

ENV CMAKE_DIR "/opt/cmake"
ENV CMAKE_VERSION "3.19.1"
ENV PATH "${CMAKE_DIR}/bin:${PATH}"

RUN apt-get update && apt-get -y install \
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
  && rm -rf /var/lib/apt/lists/*

# Recent CMake.
RUN mkdir -p "${CMAKE_DIR}" \
  && curl -sSL "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-Linux-x86_64.tar.gz" | tar xzf - -C "${CMAKE_DIR}" --strip-components 1

RUN update-alternatives --install /usr/bin/cc cc /usr/bin/gcc-8 100
RUN update-alternatives --install /usr/bin/c++ c++ /usr/bin/g++-8 100
