FROM quay.io/centos/centos:stream8

# A version field to invalidate Cirrus's build cache when needed, as suggested in
# https://github.com/cirruslabs/cirrus-ci-docs/issues/544#issuecomment-566066822
ENV DOCKERFILE_VERSION 20220519

RUN dnf -y install \
    cmake \
    diffutils \
    gcc \
    gcc-c++ \
    git \
    make \
    openssl \
    openssl-devel \
    python38 \
    python38-devel \
    python38-pip\
  && dnf clean all && rm -rf /var/cache/dnf
