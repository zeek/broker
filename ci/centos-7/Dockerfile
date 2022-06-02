FROM centos:7

# Disabled lookup of fastest mirror since the list seems to be outdated and no valid mirror can be detected.
RUN sed -i 's/enabled=1/enabled=0/' /etc/yum/pluginconf.d/fastestmirror.conf

# The version of git in the standard repos is 1.8 and CI needs 2.3+
# for the use of GIT_SSH_COMMAND when cloning private repos.
RUN yum -y install \
    https://repo.ius.io/ius-release-el7.rpm \
    https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm \
  && yum -y install git236 ccache \
  && yum clean all && rm -rf /var/cache/yum

RUN yum -y install \
    epel-release \
  && yum clean all && rm -rf /var/cache/yum

RUN yum -y install \
    centos-release-scl \
  && yum clean all && rm -rf /var/cache/yum

RUN yum -y install \
    devtoolset-8 \
  && yum clean all && rm -rf /var/cache/yum

RUN yum install -y \
    cmake3 \
    diffutils \
    make \
    openssl-devel \
    python3 \
    python3-devel \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN echo 'unset BASH_ENV PROMPT_COMMAND ENV' > /usr/bin/zeek-ci-env && \
    echo 'source /opt/rh/devtoolset-8/enable' >> /usr/bin/zeek-ci-env && \
    echo 'export PATH=${PATH}:${FLEX_DIR}/bin' >> /usr/bin/zeek-ci-env

ENV BASH_ENV="/usr/bin/zeek-ci-env" \
    ENV="/usr/bin/zeek-ci-env" \
    PROMPT_COMMAND=". /usr/bin/zeek-ci-env"
