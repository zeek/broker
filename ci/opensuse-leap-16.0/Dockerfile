FROM opensuse/leap:16.0

# A version field to invalidate Cirrus's build cache when needed, as suggested in
# https://github.com/cirruslabs/cirrus-ci-docs/issues/544#issuecomment-566066822
ENV DOCKERFILE_VERSION=20260607

RUN zypper in -y \
    cmake \
    gcc \
    gcc-c++ \
    git \
    libopenssl-devel \
    make \
    python313 \
    python313-devel \
  && rm -rf /var/cache/zypp
