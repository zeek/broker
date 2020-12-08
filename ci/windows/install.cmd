choco install -y --no-progress --ignore-package-exit-codes ^
  visualstudio2019buildtools --package-parameters "--includeRecommended --includeOptional" ^
  visualstudio2019-workload-vctools ^
  cmake --installargs 'ADD_CMAKE_TO_PATH=System' ^
  openssl
