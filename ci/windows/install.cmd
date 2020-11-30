choco install -y --no-progress --ignore-package-exit-codes ^
  visualstudio2019community ^
  visualstudio2019buildtools ^
  visualstudio2019-workload-nativedesktop ^
  cmake --installargs 'ADD_CMAKE_TO_PATH=System' ^
  openssl
