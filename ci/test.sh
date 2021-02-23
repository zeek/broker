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

cd build

if [[ -z "${BROKER_CI_MEMCHECK}" ]]; then
    $CTestCommand --output-on-failure
else
    # Python tests under ASan are problematic for various reasons, so skip
    # e.g. need LD_PRELOAD, some specific compiler packagings are
    # buggy when preloaded due to missing libasan compilation flag,
    # leaks are miss-reported so have to disable leak checking, and
    # finally most tests end up timing out when run under ASan anyway.
    $CTestCommand --output-on-failure -E python
fi
