FROM opensuse/tumbleweed

# A version field to invalide Cirrus's build cache when needed, as suggested in
# https://github.com/cirruslabs/cirrus-ci-docs/issues/544#issuecomment-566066822
ENV DOCKERFILE_VERSION 20230329

RUN zypper in -y \
    cmake \
    gcc \
    gcc-c++ \
    git \
    libopenssl-devel \
    make \
    python3 \
    python3-devel \
  && rm -rf /var/cache/zypp
