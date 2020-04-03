@echo on

echo %BROKER_CI_CPUS%
wmic cpu get NumberOfCores, NumberOfLogicalProcessors/Format:List
systeminfo

choco install -y --no-progress openssl
