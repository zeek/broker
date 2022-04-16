:: See build.cmd for documentation on this call.
call "c:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x86_amd64

cd build
ctest -C release || exit \b 1
