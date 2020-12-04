FROM centos:7

RUN yum install -y centos-release-scl \
    && yum install -y epel-release \
    && yum update -y \
    && yum clean all

RUN yum install -y \
    cmake3 \
    devtoolset-7 \
    git \
    make \
    openssl-devel \
    python3 \
    python3-devel \
    && yum clean all \
    && rm -rf /var/cache/yum

ENV CC=/opt/rh/devtoolset-7/root/usr/bin/gcc
ENV CXX=/opt/rh/devtoolset-7/root/usr/bin/g++
