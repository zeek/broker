#!/bin/sh

echo "Preparing macOS environment"
sysctl hw.model hw.machine hw.ncpu hw.physicalcpu hw.logicalcpu
set -e
set -x

brew install cmake openssl python3

pip3 install virtualenv
virtualenv -p /usr/local/bin/python3 ~/.virtualenv/py3
source ~/.virtualenv/py3/bin/activate
