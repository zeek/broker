FROM centos:7

RUN yum -y install \
    epel-release \
  && yum clean all && rm -rf /var/cache/yum

RUN yum -y install \
    centos-release-scl \
  && yum clean all && rm -rf /var/cache/yum

RUN yum -y install \
    devtoolset-7 \
  && yum clean all && rm -rf /var/cache/yum

RUN yum -y install \
    git \
    cmake3 \
    make \
    openssl \
    openssl-devel \
    python3 \
    python3-devel \
  && yum clean all && rm -rf /var/cache/yum

RUN echo 'unset BASH_ENV PROMPT_COMMAND ENV' > /usr/bin/broker-ci-env && \
    echo 'source /opt/rh/devtoolset-7/enable' >> /usr/bin/broker-ci-env

ENV BASH_ENV="/usr/bin/broker-ci-env" \
    ENV="/usr/bin/broker-ci-env" \
    PROMPT_COMMAND=". /usr/bin/broker-ci-env"
