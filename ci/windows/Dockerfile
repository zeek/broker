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

SHELL [ "powershell" ]

RUN Set-ExecutionPolicy Unrestricted -Force

# Install Chocolatey
RUN [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; `
    iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))

# Install the Visual Studio tooling
RUN choco install -y --no-progress visualstudio2022buildtools
RUN choco install -y --no-progress visualstudio2022-workload-vctools

# Install some dependencies that aren't included in the MSVC tools
RUN choco install -y --no-progress openssl --version=3.1.1
RUN choco install -y --no-progress python
RUN choco install -y --no-progress msysgit

# Add git to the global PATH
SHELL [ "cmd", "/c" ]
RUN setx /M PATH "%PATH%;C:\\Program Files\\Git\\bin"