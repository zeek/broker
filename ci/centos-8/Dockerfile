FROM centos:8

# Work around a hopefully transient cmake-specific problem:
# "cmake3: symbol lookup error: cmake3: undefined symbol: archive_write_add_filter_zstd"
RUN yum update -y libarchive

RUN yum install -y \
    cmake \
    gcc \
    gcc-c++ \
    git \
    make \
    openssl-devel \
    python3 \
    python3-devel \
    && yum clean all \
    && rm -rf /var/cache/yum
