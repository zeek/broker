FROM opensuse/leap:15.4

# A version field to invalidate Cirrus's build cache when needed, as suggested in
# https://github.com/cirruslabs/cirrus-ci-docs/issues/544#issuecomment-566066822
ENV DOCKERFILE_VERSION 20220615

RUN zypper in -y \
    cmake \
    gcc \
    gcc-c++ \
    git \
    gzip \
    libopenssl-devel \
    make \
    python39 \
    python39-devel \
    python39-pip \
    tar \
  && rm -rf /var/cache/zypp

RUN update-alternatives --install /usr/bin/pip3 pip3 /usr/bin/pip3.9 100
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 100
