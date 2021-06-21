FROM fedora:34

RUN dnf install -y \
    cmake \
    gcc \
    gcc-c++ \
    git \
    make \
    openssl \
    openssl-devel \
    python3 \
    python3-devel \
    && dnf clean all \
    && rm -rf /var/cache/dnf
