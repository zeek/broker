FROM fedora:33

RUN yum install -y \
    cmake \
    gcc \
    gcc-c++ \
    git \
    make \
    openssl \
    openssl-devel \
    python3 \
    python3-devel \
    && yum clean all \
    && rm -rf /var/cache/yum
