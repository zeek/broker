# CMake dependencies.
include(FetchContent)

# Override this path
set(ZEEK_BUNDLE_PREFIX "${PROJECT_BINARY_DIR}/zeek-bundle" CACHE STRING
    "Path to the Zeek packages cache directory")

# Opt-in to changed CMake behavior for fetch ZIP files.
if (POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif ()

# Tries to find a package in the Zeek package cache.
function(ZeekBundle_Find name)
  find_package(${name} QUIET NO_DEFAULT_PATH HINTS ${ZEEK_BUNDLE_PREFIX})
  if (${name}_FOUND)
    set(ZeekBundle_FOUND ON PARENT_SCOPE)
  else()
    set(ZeekBundle_FOUND OFF PARENT_SCOPE)
  endif ()
endfunction()

# Builds an external project at configure time.
function(ZeekBundle_Build name srcDir binDir)
  # Extra arguments are passed as CMake options to the external project.
  set(cmakeArgs "")
  foreach (arg IN LISTS ARGN)
    list(APPEND cmakeArgs "-D${arg}")
  endforeach ()
  # Run CMake for the external project.
  message(STATUS "Configuring bundled project: ${name}")
  execute_process(
    COMMAND
      "${CMAKE_COMMAND}"
      -G "${CMAKE_GENERATOR}"
      ${cmakeArgs}
      -DCMAKE_BUILD_TYPE=Release
      -DBUILD_SHARED_LIBS=OFF
      -DENABLE_TESTING=OFF
      "-DCMAKE_INSTALL_PREFIX=${ZEEK_BUNDLE_PREFIX}"
      "${srcDir}"
    WORKING_DIRECTORY "${binDir}"
    OUTPUT_FILE "${binDir}/cmake.out"
    ERROR_FILE "${binDir}/cmake.err"
    RESULT_VARIABLE cmakeResult)
  if (NOT cmakeResult EQUAL 0)
    message(FATAL_ERROR "Failed to configure external project: ${srcDir}!\nSee ${binDir}/cmake.out and ${binDir}/cmake.err for details.")
  endif ()
  # Build the external project.
  message(STATUS "Building bundled project: ${name}")
  execute_process(
    COMMAND
      "${CMAKE_COMMAND}"
      --build "${binDir}"
      --config Release
    OUTPUT_FILE "${binDir}/build.out"
    ERROR_FILE "${binDir}/build.err"
    RESULT_VARIABLE buildResult)
  if (NOT buildResult EQUAL 0)
    message(FATAL_ERROR "Failed to build external project: ${srcDir}!\nSee ${binDir}/build.out and ${binDir}/build.err for details.")
  endif ()
  # Install the external project.
  message(STATUS "Installing bundled project: ${name}")
  execute_process(
    COMMAND
      "${CMAKE_COMMAND}"
      --build "${binDir}"
      --config Release
      --target install
    OUTPUT_FILE "${binDir}/install.out"
    ERROR_FILE "${binDir}/install.err"
    RESULT_VARIABLE installResult)
  if (NOT installResult EQUAL 0)
    message(FATAL_ERROR "Failed to install external project: ${srcDir}!\nSee ${binDir}/install.out and ${binDir}/install.err for details.")
  endif ()
endfunction ()

# Adds a bundled package to Zeek.
#
# Usage:
# ZeekBundle_Add(
#   NAME <name>
#   FETCH (URL <url> | GIT_REPOSITORY <repo> GIT_TAG <tag> | SOURCE_DIR <path>)
#   [CONFIGURE <args>]
# )
function (ZeekBundle_Add)
  # Parse the function arguments.
  cmake_parse_arguments(arg "" "NAME" "FETCH;CONFIGURE" ${ARGN})
  if (NOT arg_NAME)
    message(FATAL_ERROR "ZeekBundle_Add: mandatory argument NAME is missing!")
  endif ()
  if (NOT arg_FETCH)
    message(FATAL_ERROR "ZeekBundle_Add: mandatory argument FETCH is missing!")
  endif ()
  # Use find_package if the user explicitly requested the package.
  if (DEFINED ${arg_NAME}_ROOT)
    message(STATUS "ZeekBundle: use system library for ${arg_NAME}")
    find_package(${arg_NAME} QUIET REQUIRED NO_DEFAULT_PATH PATHS ${${arg_NAME}_ROOT})
    return ()
  endif ()
  # Check if we already have the package.
  ZeekBundle_Find(${arg_NAME})
  if (ZeekBundle_FOUND)
    return ()
  endif ()
  # Fetch the package by delegating to FetchContent.
  FetchContent_Declare(dl_${arg_NAME} ${arg_FETCH})
  string(TOLOWER "dl_${arg_NAME}" internalName)
  FetchContent_Populate(${internalName})
  # Build the package and verify that it was found.
  ZeekBundle_Build(
    ${arg_NAME}
    "${${internalName}_SOURCE_DIR}"
    "${${internalName}_BINARY_DIR}"
    ${arg_CONFIGURE})
  find_package(${arg_NAME} QUIET REQUIRED NO_DEFAULT_PATH PATHS ${ZEEK_BUNDLE_PREFIX})
endfunction ()
