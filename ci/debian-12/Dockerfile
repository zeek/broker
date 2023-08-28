FROM debian:12

ENV DEBIAN_FRONTEND="noninteractive" TZ="America/Los_Angeles"

# A version field to invalidate Cirrus's build cache when needed, as suggested in
# https://github.com/cirruslabs/cirrus-ci-docs/issues/544#issuecomment-566066822
ENV DOCKERFILE_VERSION 20230612

RUN apt-get update && apt-get -y install \
    clang \
    clang-tidy \
    cmake \
    curl \
    g++ \
    gcc \
    git \
    libssl-dev \
    make \
    python3 \
    python3-dev \
    python3-pip \
  && apt autoclean \
  && rm -rf /var/lib/apt/lists/*

# Allow pip to interfere with externally managed environment:
# https://peps.python.org/pep-0668/
# https://stackoverflow.com/q/75608323
RUN rm -f /usr/lib/python3.*/EXTERNALLY-MANAGED
