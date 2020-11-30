@echo on

set PATH=%PATH%;C:\Program Files\CMake\bin
echo %BROKER_CI_CPUS%
wmic cpu get NumberOfCores, NumberOfLogicalProcessors/Format:List
systeminfo
cmake --version
