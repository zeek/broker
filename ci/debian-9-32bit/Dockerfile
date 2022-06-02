FROM i386/debian:9

ENV CMAKE_DIR "/opt/cmake"
ENV CMAKE_VERSION "3.19.1"
ENV PATH "${CMAKE_DIR}/bin:${PATH}"

RUN apt update -y \
    && apt install -y \
       curl \
       clang-7 \
       git \
       libc++-7-dev \
       libc++abi-7-dev \
       libssl-dev \
       make \
       python3 \
       python3-dev \
    && apt autoclean \
    && rm -rf /var/lib/apt/lists/* \
    # Recent CMake.
    && mkdir -p "${CMAKE_DIR}" \
    && curl -sSL "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-Linux-x86_64.tar.gz" | tar xzf - -C "${CMAKE_DIR}" --strip-components 1

RUN update-alternatives --install /usr/bin/cc cc /usr/bin/clang-7 100
RUN update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-7 100

ENV CXXFLAGS=-stdlib=libc++
