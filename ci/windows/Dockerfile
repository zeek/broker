# escape=`

# Note that this Dockerfile is mostly just an example for manual
# testing/development purposes.  CI does not currently use it.

# Note that VS BuildTools install is sensitive to available memory and disk
# space and seems to have non-obvious exit codes or error messages that would
# otherwise help indicate that's potential reason for botched installation.
# Here's an example (re)configuration of docker that resulted in a good image:
#
#   net stop docker
#   dockerd --unregister-service
#   dockerd --register-service --storage-opt size=64G
#   net start docker
#   docker build -t buildtools2019:latest -m 2GB .

FROM mcr.microsoft.com/dotnet/framework/sdk:4.8-windowsservercore-ltsc2019

# Restore the default Windows shell for correct batch processing.
SHELL ["cmd", "/S", "/C"]

# From Cirrus CI base image:
# https://github.com/cirruslabs/docker-images-windows/blob/master/windowsservercore/Dockerfile
RUN powershell -NoLogo -NoProfile -Command `
    netsh interface ipv4 show interfaces ; `
    netsh interface ipv4 set subinterface 18 mtu=1460 store=persistent ; `
    netsh interface ipv4 show interfaces ; `
    Set-ExecutionPolicy Bypass -Scope Process -Force; iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1')) ; `
    choco install -y --no-progress git 7zip ; `
    Remove-Item C:\ProgramData\chocolatey\logs -Force -Recurse ; `
    Remove-Item C:\Users\ContainerAdministrator\AppData\Local\Temp -Force -Recurse

# OpenSSL has to be installed separately from the above here or
# the call to install the MSVC bits fails for some reason. Python
# is separate so we can specify a version.
RUN choco install -y --no-progress openssl
RUN choco install -y --no-progress --version=3.10.0 python

# Download the Build Tools bootstrapper.
ADD https://aka.ms/vs/17/release/vs_buildtools.exe C:\TEMP\vs_buildtools.exe

# Install Build Tools and additional workloads, excluding workloads and
# components with known issues.  Based on example from:
# https://docs.microsoft.com/en-us/visualstudio/install/build-tools-container?view=vs-2019
RUN C:\TEMP\vs_buildtools.exe --quiet --wait --norestart --nocache `
    --add Microsoft.VisualStudio.Workload.VCTools `
    --add Microsoft.VisualStudio.Component.VC.Tools.x86.x64 `
    --add Microsoft.VisualStudio.Component.VC.CMake.Project `
    --add Microsoft.VisualStudio.Component.Windows10SDK.19041 `
    --add Microsoft.VisualStudio.Component.TestTools.BuildTools `
    --remove Microsoft.VisualStudio.Component.Windows10SDK.10240 `
    --remove Microsoft.VisualStudio.Component.Windows10SDK.10586 `
    --remove Microsoft.VisualStudio.Component.Windows10SDK.14393 `
    --remove Microsoft.VisualStudio.Component.Windows81SDK `
 || IF "%ERRORLEVEL%"=="3010" EXIT 0
