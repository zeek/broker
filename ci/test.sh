#! /usr/bin/env bash

set -e

cd build

if [[ -z "${BROKER_CI_MEMCHECK}" ]]; then
    CTEST_OUTPUT_ON_FAILURE=1 make test
else
    # Python tests under ASan are problematic for various reasons, so skip
    # e.g. need LD_PRELOAD, some specific compiler packagings are
    # buggy when preloaded due to missing libasan compilation flag,
    # leaks are miss-reported so have to disable leak checking, and
    # finally most tests end up timing out when run under ASan anyway.
    CTEST_OUTPUT_ON_FAILURE=1 ctest -E python
fi
