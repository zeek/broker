FROM fedora:37

# A version field to invalide Cirrus's build cache when needed, as suggested in
# https://github.com/cirruslabs/cirrus-ci-docs/issues/544#issuecomment-566066822
ENV DOCKERFILE_VERSION 20221127

RUN dnf -y install \
    clang \
    clang-tools-extra \
    cmake \
    diffutils \
    gcc \
    gcc-c++ \
    git \
    make \
    openssl \
    openssl-devel \
    python3 \
    python3-devel \
  && dnf clean all && rm -rf /var/cache/dnf
