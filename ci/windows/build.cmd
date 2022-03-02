call "c:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x86_amd64

mkdir build
cd build

:: All of the relevant binaries here are put in the path by the bat file above
C:\WINDOWS\system32\cmd.exe /c %SYSTEMROOT%\System32\chcp.com 65001 >NUL && ^
cmake.exe -G "Ninja" -DCMAKE_BUILD_TYPE:STRING="Debug" ^
-DCMAKE_C_COMPILER=cl.exe -DCMAKE_CXX_COMPILER=cl.exe -DCMAKE_MAKE_PROGRAM=ninja.exe ^
-DOPENSSL_ROOT_DIR="C:\Program Files\OpenSSL-Win64" -DEXTRA_FLAGS=/MP%BROKER_CI_CPUS% ..

cmake.exe --build . --target install --config release || exit \b 1
cd ..
