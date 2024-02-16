#! /usr/bin/env bash

# Prefer ctest3 over "regular" ctest (ctest == ctest2 on RHEL).
if command -v ctest3 >/dev/null 2>&1 ; then
  CTestCommand="ctest3"
elif command -v ctest >/dev/null 2>&1 ; then
  CTestCommand="ctest"
else
  echo "No ctest command found!"
  exit 1
fi

set -e

result=0

export PATH="$PATH:$PWD/build/bin"

BaseDir="$PWD"

cd build

if [[ -z "${BROKER_CI_MEMCHECK}" ]]; then
    # C++ test suites (via CTest).
    $CTestCommand --output-on-failure
else
    # Python tests under ASan are problematic for various reasons, so skip
    # e.g. need LD_PRELOAD, some specific compiler packagings are
    # buggy when preloaded due to missing libasan compilation flag,
    # leaks are miss-reported so have to disable leak checking, and
    # finally most tests end up timing out when run under ASan anyway.
    $CTestCommand --output-on-failure -E python
fi

# BTest suites. Those also use Python but they simply execute Broker binaries
# as-is and are thus OK to use under ASAN.
if command -v pip3 >/dev/null 2>&1 ; then
    BinDir="$(python3 -m site --user-base)/bin"
    export PATH="$PATH:$BinDir"
    python3 -m venv test-env
    source test-env/bin/activate
    pip3 install btest websockets
    cd $BaseDir/tests/btest
    btest || result=1
    [[ -d .tmp ]] && tar -czf tmp.tar.gz .tmp
fi

exit ${result}
