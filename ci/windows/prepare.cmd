@echo on

echo %BROKER_CI_CPUS%
wmic cpu get NumberOfCores, NumberOfLogicalProcessors/Format:List
systeminfo
dir C:
choco list
