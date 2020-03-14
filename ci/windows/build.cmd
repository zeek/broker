set PATH=%PATH%;C:\Program Files\CMake\bin
mkdir build
cd build
cmake -A x64 -DOPENSSL_ROOT_DIR="C:\Program Files\OpenSSL-Win64" .. || exit \b 1
cmake --build . --target install --config release -j %BROKER_CI_CPUS% || exit \b 1
cd ..
