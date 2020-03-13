FROM fedora:30

RUN yum -y install \
    git \
    cmake \
    make \
    gcc \
    gcc-c++ \
    openssl \
    openssl-devel \
    python3 \
    python3-devel \
  && yum clean all && rm -rf /var/cache/yum
