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

RUN choco install -y --no-progress --installargs 'ADD_CMAKE_TO_PATH=System' cmake openssl

Download the Build Tools bootstrapper.
ADD https://aka.ms/vs/16/release/vs_buildtools.exe C:\TEMP\vs_buildtools.exe

Install Build Tools and additional workloads, excluding workloads and
components with known issues.  Based on example from:
https://docs.microsoft.com/en-us/visualstudio/install/build-tools-container?view=vs-2019
RUN C:\TEMP\vs_buildtools.exe --quiet --wait --norestart --nocache `
    --installPath C:\BuildTools `
    --add Microsoft.VisualStudio.Workload.VCTools `
    --add Microsoft.VisualStudio.Component.VC.Tools.x86.x64 `
    --add Microsoft.VisualStudio.Component.Windows10SDK.18362 `
    --remove Microsoft.VisualStudio.Component.Windows10SDK.10240 `
    --remove Microsoft.VisualStudio.Component.Windows10SDK.10586 `
    --remove Microsoft.VisualStudio.Component.Windows10SDK.14393 `
    --remove Microsoft.VisualStudio.Component.Windows81SDK `
 || IF "%ERRORLEVEL%"=="3010" EXIT 0

# This entry point starts the developer command prompt and launches PowerShell.
ENTRYPOINT ["C:\\BuildTools\\Common7\\Tools\\VsDevCmd.bat", "-arch=amd64", "&&", "powershell.exe", "-NoLogo", "-ExecutionPolicy", "Bypass"]
