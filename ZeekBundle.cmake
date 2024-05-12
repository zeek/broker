# CMake dependencies.
include(FetchContent)

# Users may override this path to place the compiled packages elsewhere.
set(ZEEK_BUNDLE_PREFIX "${PROJECT_BINARY_DIR}/zeek-bundle" CACHE STRING
    "Path to the Zeek packages cache directory")

# Opt-in to changed CMake behavior for fetch ZIP files.
if (POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif ()

# Tries to find a package in the Zeek package cache.
function(ZeekBundle_Find name)
  find_package(${name} CONFIG QUIET NO_DEFAULT_PATH HINTS ${ZEEK_BUNDLE_PREFIX})
  if (${name}_FOUND)
    set(ZeekBundle_FOUND ON PARENT_SCOPE)
  else()
    set(ZeekBundle_FOUND OFF PARENT_SCOPE)
  endif ()
endfunction()

# Runs the build and install steps for an external project.
function(ZeekBundle_BuildStep name type binDir)
  # Build the external project.
  message(STATUS "Building bundled project: ${name} as ${type}")
  execute_process(
    COMMAND
      "${CMAKE_COMMAND}"
      --build "${binDir}"
      --config ${type}
    OUTPUT_FILE "${binDir}/build${type}.out"
    ERROR_FILE "${binDir}/build${type}.err"
    RESULT_VARIABLE buildResult)
  if (NOT buildResult EQUAL 0)
    file(READ "${binDir}/build${type}.err" cmakeErr)
    message(FATAL_ERROR "Failed to build ${name}:\n\n${cmakeErr}")
  endif ()
  # Install the external project.
  message(STATUS "Installing bundled project: ${name} as ${type}")
  execute_process(
    COMMAND
      "${CMAKE_COMMAND}"
      --build "${binDir}"
      --config ${type}
      --target install
    OUTPUT_FILE "${binDir}/install${type}.out"
    ERROR_FILE "${binDir}/install${type}.err"
    RESULT_VARIABLE installResult)
  if (NOT installResult EQUAL 0)
    file(READ "${binDir}/install${type}.err" cmakeErr)
    message(FATAL_ERROR "Failed to install ${name}:\n\n${cmakeErr}")
  endif ()
endfunction()

# Builds an external project at configure time.
function(ZeekBundle_Build name srcDir binDir)
  # Extra arguments are passed as CMake options to the external project.
  set(cmakeArgs "")
  foreach (arg IN LISTS ARGN)
    list(APPEND cmakeArgs "-D${arg}")
  endforeach ()
  # Make sure we can build debug and release versions of the project separately.
  list(APPEND cmakeArgs "-DCMAKE_DEBUG_POSTFIX=d")
  # Run CMake for the external project.
  message(STATUS "Configuring bundled project: ${name}")
  execute_process(
    COMMAND
      "${CMAKE_COMMAND}"
      -G "${CMAKE_GENERATOR}"
      ${cmakeArgs}
      -DBUILD_SHARED_LIBS=OFF
      -DCMAKE_POSITION_INDEPENDENT_CODE=ON
      -DENABLE_TESTING=OFF
      "-DCMAKE_INSTALL_PREFIX=${ZEEK_BUNDLE_PREFIX}"
      "${srcDir}"
    WORKING_DIRECTORY "${binDir}"
    OUTPUT_FILE "${binDir}/cmake.out"
    ERROR_FILE "${binDir}/cmake.err"
    RESULT_VARIABLE cmakeResult)
  if (NOT cmakeResult EQUAL 0)
    file(READ "${binDir}/cmake.err" cmakeErr)
    message(FATAL_ERROR "Failed to configure external project ${name}:\n\n${cmakeErr}")
  endif ()
  get_property(isMultiConfig GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)
  if (isMultiConfig)
    ZeekBundle_BuildStep(${name} Debug ${binDir})
    ZeekBundle_BuildStep(${name} Release ${binDir})
  else ()
    # On Windows, we cannot mix debug and release libraries.
    if (WIN32)
      ZeekBundle_BuildStep(${name} ${CMAKE_BUILD_TYPE} ${binDir})
    else()
      ZeekBundle_BuildStep(${name} Release ${binDir})
    endif()
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
    find_package(${arg_NAME} CONFIG QUIET REQUIRED NO_DEFAULT_PATH PATHS ${${arg_NAME}_ROOT})
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
  find_package(${arg_NAME} CONFIG QUIET REQUIRED NO_DEFAULT_PATH PATHS ${ZEEK_BUNDLE_PREFIX})
endfunction ()
