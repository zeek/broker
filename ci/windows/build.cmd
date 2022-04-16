:: Import the visual studio compiler environment into the one running in the
:: cmd current shell. This path is hard coded to the one on the CI image, but
:: can be adjusted if running builds locally. Unfortunately, the initial path
:: isn't in the environment so we have to hardcode the whole path.
call "c:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x86_amd64

mkdir build
cd build

:: The configure script doesn't work for Windows but we can call cmake directly
:: to do the same. Ninja is installed as part of the CI image.
C:\WINDOWS\system32\cmd.exe /c %SYSTEMROOT%\System32\chcp.com 65001 >NUL && ^
cmake.exe -G "Ninja" -DCMAKE_BUILD_TYPE:STRING="Debug" ^
-DCMAKE_C_COMPILER=cl.exe -DCMAKE_CXX_COMPILER=cl.exe -DCMAKE_MAKE_PROGRAM=ninja.exe ^
-DOPENSSL_ROOT_DIR="C:\Program Files\OpenSSL-Win64" -DEXTRA_FLAGS=/MP%BROKER_CI_CPUS% ^
-DDISABLE_PYTHON_BINDINGS="true" ..

cmake.exe --build . --target install --config release || exit \b 1
cd ..
