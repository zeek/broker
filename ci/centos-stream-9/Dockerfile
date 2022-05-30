FROM quay.io/centos/centos:stream9

RUN dnf install -y \
    cmake \
    diffutils \
    gcc \
    gcc-c++ \
    git \
    make \
    openssl-devel \
    python3 \
    python3-devel \
    && dnf clean all \
    && rm -rf /var/cache/dnf
