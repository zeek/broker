Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "Installing dependencies via Chocolatey"

choco install -y --no-progress visualstudio2022buildtools --package-parameters "--add Microsoft.VisualStudio.Workload.VCTools --includeRecommended"
choco install -y --no-progress openssl --version=3.1.1
choco install -y --no-progress python
choco install -y --no-progress ninja

Write-Host "Dependency installation complete"
