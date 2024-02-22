This directory contains internal Broker headers that CMake will *not* install.
Hence, other headers *must not* include any header from this directory (except
other internal headers).

Including internal headers in sources files is always OK.
