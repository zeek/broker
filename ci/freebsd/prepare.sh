#!/bin/sh

echo "Preparing FreeBSD environment"
sysctl hw.model hw.machine hw.ncpu
set -e
set -x

env ASSUME_ALWAYS_YES=YES pkg bootstrap
pkg install -y bash git cmake python3

# We do have a unit test that checks the behavior of auto-reconnects
# by attempted to connect to an unused port.  The FreeBSD image used
# by Cirrus sets this following option such that those connections do
# not get an immediate RST, but instead wait for ~75 seconds to time out.
# That's not ideal for the test, so undo that setting.
sysctl -w net.inet.tcp.blackhole=0
