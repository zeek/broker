:: Import the visual studio compiler environment into the one running in the
:: cmd current shell. This path is hard coded to the one on the CI image, but
:: can be adjusted if running builds locally. Unfortunately, the initial path
:: isn't in the environment so we have to hardcode the whole path.
call "c:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x86_amd64

mkdir build
cd build

cmake.exe .. -DCMAKE_BUILD_TYPE=debug -G Ninja ^
   -DOPENSSL_ROOT_DIR="C:\Program Files\OpenSSL-Win64" -DEXTRA_FLAGS=/MP%BROKER_CI_CPUS% ^
   -DDISABLE_PYTHON_BINDINGS:BOOL=ON -DDISABLE_BROKER_EXTRA_TOOLS:BOOL=ON

cmake.exe --build . --target install --config release || exit \b 1

:: Back up one directory so that we can run tests from the top-level directory
cd ..
