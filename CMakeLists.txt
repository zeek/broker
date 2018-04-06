# -- Project Setup ------------------------------------------------------------

project(broker C CXX)
cmake_minimum_required(VERSION 3.0.2)
include(cmake/CommonCMakeConfig.cmake)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/lib)

# -- Dependencies -------------------------------------------------------------

if ( CAF_ROOT_DIR )
  find_package(CAF COMPONENTS core io test openssl REQUIRED)
  set(caf_dir    "${CAF_LIBRARY_CORE}")
  set(caf_config "${CAF_INCLUDE_DIR_CORE}/caf/config.hpp")
  get_filename_component(caf_dir ${caf_dir} PATH)

  if (EXISTS "${caf_dir}/../libcaf_core")
    get_filename_component(caf_dir ${caf_dir} PATH)
  else ()
    set(caf_dir "${CAF_INCLUDE_DIR_CORE}")
  endif ()
else ()
  include(ExternalProject)
  set(caf_module  "${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/caf")
  set(caf_ep      "${CMAKE_CURRENT_BINARY_DIR}/caf-ep")
  set(caf_build   "${CMAKE_CURRENT_BINARY_DIR}/caf-build")
  set(caf_src     "${CMAKE_CURRENT_BINARY_DIR}/caf-src")
  set(caf_dir     "${caf_module}")
  set(caf_install "${CMAKE_INSTALL_PREFIX}")
  set(shared_ext  "${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(static_ext  "${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(caf_config  "${caf_module}/libcaf_core/caf/config.hpp")

  # For now, if you want to build a static lib, just go through
  # the --with-caf path and point to separate, custom-built CAF.
  ExternalProject_Add(project_caf
    PREFIX     "${caf_ep}"
    BINARY_DIR "${caf_build}"
    SOURCE_DIR "${caf_src}"
    URL        "${caf_module}"
    CMAKE_CACHE_ARGS
      -DCAF_NO_UNIT_TESTS:bool=yes
      -DCAF_NO_EXAMPLES:bool=yes
      -DCAF_NO_TOOLS:bool=yes
      -DCAF_NO_BENCHMARKS:bool=yes
      -DCAF_NO_PYTHON:bool=yes
      -DCAF_NO_OPENCL:bool=yes
      # Sticking CAF libs in the same output dir as libbroker is a bit
      # of a hack until CMake 3.8, which has BUILD_RPATH that can instead
      # be set on libbroker.  The problem here is that broker unit tests
      # will fail if linker can't locate CAF libs (e.g. via build-tree
      # RPATH).
      -DCMAKE_LIBRARY_OUTPUT_DIRECTORY:path=${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
      -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY:path=${CMAKE_ARCHIVE_OUTPUT_DIRECTORY}
      -DCMAKE_INSTALL_PREFIX:path=${caf_install}
    INSTALL_COMMAND cmake -E echo "Skip CAF external project install."
    # Build byproducts is just required by the Ninja generator
    # though it's not available before CMake 3.2 ...
    BUILD_BYPRODUCTS
      ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libcaf_core${shared_ext}
      ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libcaf_io${shared_ext}
      ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libcaf_openssl${shared_ext}
      ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libcaf_core${static_ext}
      ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libcaf_io${static_ext}
      ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libcaf_openssl${static_ext}
  )
  install(CODE "execute_process(
    COMMAND ${CMAKE_MAKE_PROGRAM} install
    WORKING_DIRECTORY ${caf_build}
    )"
  )

  add_library(libcaf_core_shared SHARED IMPORTED)
  add_library(libcaf_io_shared SHARED IMPORTED)
  add_library(libcaf_openssl_shared SHARED IMPORTED)
  set_property(TARGET libcaf_core_shared PROPERTY IMPORTED_LOCATION
               ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libcaf_core${shared_ext})
  set_property(TARGET libcaf_io_shared PROPERTY IMPORTED_LOCATION
               ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libcaf_io${shared_ext})
  set_property(TARGET libcaf_openssl_shared PROPERTY IMPORTED_LOCATION
               ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libcaf_openssl${shared_ext})
  add_dependencies(libcaf_core_shared project_caf)
  add_dependencies(libcaf_io_shared project_caf)
  add_dependencies(libcaf_openssl_shared project_caf)

  set(CAF_FOUND true)
  set(CAF_LIBRARIES
      libcaf_core_shared libcaf_io_shared libcaf_openssl_shared
      CACHE STRING "CAF libs" FORCE)

  set(CAF_INCLUDE_DIRS
      "${caf_src}/libcaf_core"
      "${caf_src}/libcaf_io"
      "${caf_src}/libcaf_openssl"
      "${caf_src}/libcaf_test"
      CACHE STRING "CAF includes" FORCE)

  set(CAF_INCLUDE_DIR_CORE
      "${caf_src}/libcaf_core" CACHE PATH "CAF core headers" FORCE)
  set(CAF_INCLUDE_DIR_IO
      "${caf_src}/libcaf_io" CACHE PATH "CAF io headers" FORCE)
  set(CAF_INCLUDE_DIR_OPENSSL
      "${caf_src}/libcaf_openssl" CACHE PATH "CAF openssl headers" FORCE)
endif ()

file(READ "${caf_config}" caf_config_hpp)
string(REGEX MATCH "#define CAF_VERSION [0-9]+"
       caf_version_line "${caf_config_hpp}")
string(REGEX MATCH "[0-9]+" caf_version_int "${caf_version_line}")
math(EXPR caf_version_major "${caf_version_int} / 10000")
math(EXPR caf_version_minor "( ${caf_version_int} / 100) % 100")
math(EXPR caf_version_patch "${caf_version_int} % 100")
set(CAF_VERSION
    "${caf_version_major}.${caf_version_minor}.${caf_version_patch}")

set(CAF_VERSION_REQUIRED 0.15.5)

if (CAF_VERSION VERSION_LESS ${CAF_VERSION_REQUIRED})
  message(FATAL_ERROR "Broker requires at least CAF version"
    " ${CAF_VERSION_REQUIRED}, detected version: ${CAF_VERSION}")
endif ()

include_directories(BEFORE ${CAF_INCLUDE_DIRS})
set(LINK_LIBS ${LINK_LIBS} ${CAF_LIBRARIES})

# OpenSSL
find_package(OpenSSL REQUIRED)
set(LINK_LIBS ${LINK_LIBS} ${OPENSSL_LIBRARIES})

# RocksDB
find_package(RocksDB)
if (ROCKSDB_FOUND)
  set(BROKER_HAVE_ROCKSDB true)
  include_directories(BEFORE ${ROCKSDB_INCLUDE_DIRS})
  set(LINK_LIBS ${LINK_LIBS} ${ROCKSDB_LIBRARIES})
  set(OPTIONAL_SRC ${OPTIONAL_SRC} src/detail/rocksdb_backend.cc)
endif ()

# -- libroker -----------------------------------------------------------------

# Extract Broker version
file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/VERSION" VERSION LIMIT_COUNT 1)
string(REPLACE "." " " _version_numbers ${VERSION})
separate_arguments(_version_numbers)
list(GET _version_numbers 0 BROKER_VERSION_MAJOR)
list(GET _version_numbers 1 BROKER_VERSION_MINOR)
list(LENGTH _version_numbers _version_length)
if (_version_length LESS 3)
  set(BROKER_VERSION_PATCH 0)
else ()
  list(GET _version_numbers 2 BROKER_VERSION_PATCH)
endif ()

set(BROKER_VERSION
    ${BROKER_VERSION_MAJOR}.${BROKER_VERSION_MINOR}.${BROKER_VERSION_PATCH})
# The SO number shall increase only if binary interface changes.
set(BROKER_SOVERSION 0)
set(ENABLE_SHARED true)

if (ENABLE_STATIC_ONLY)
  set(ENABLE_STATIC true)
  set(ENABLE_SHARED false)
endif ()

# -- compiler -----------------------------------------------------------------

if (APPLE)
  set(BROKER_APPLE true)
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(BROKER_LINUX true)
elseif(${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
  set(BROKER_FREEBSD)
endif ()

include(RequireCXX11)

set (version_cpp "
  #include <iostream>
  using namespace std\;
  int main() {
  #   ifdef __clang__
      cout << __clang_major__ << '.' << __clang_minor__\;
  #   elif defined(__GNUC__)
      cout << __GNUC__ << '.' << __GNUC_MINOR__\;
  #   else
      cout << \"0.0\"\;
  #   endif
      return 0\;
  }")
if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
  file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/version.cpp" ${version_cpp})
  if (NOT NO_AUTO_LIBCPP)
    # Standard library selection: check if libc++ is available, and if so, use
    # it. Otherwise fall back to libstdc++.
    set(CXXFLAGS_BACKUP "${CMAKE_CXX_FLAGS}")
    set(CMAKE_CXX_FLAGS "-std=c++11 -stdlib=libc++")
    try_run(program_result
            compilation_succeeded
            "${CMAKE_CURRENT_BINARY_DIR}"
            "${CMAKE_CURRENT_BINARY_DIR}/version.cpp")
    set(CMAKE_CXX_FLAGS "${CXXFLAGS_BACKUP}")
    if(compilation_succeeded)
      set (disable "(disable with --no-auto-libc++)")
      message(STATUS "Automatically adding -stdlib=libc++ flag ${disable}")
      set(EXTRA_FLAGS "${EXTRA_FLAGS} -stdlib=libc++")
    endif()
  endif()
endif ()

# Mac OS ignores -pthread but other platforms require it
if (NOT BROKER_APPLE)
  set(EXTRA_FLAGS "${EXTRA_FLAGS} -pthread")
endif ()

# Incrase warnings.
set(EXTRA_FLAGS "${EXTRA_FLAGS} -Wall -pedantic")

# Increase maximum number of instantiations.
set(EXTRA_FLAGS "${EXTRA_FLAGS} -ftemplate-depth=512")

# Reduce the number of template instantiations shown in backtrace.
set(EXTRA_FLAGS "${EXTRA_FLAGS} -ftemplate-backtrace-limit=3")

if (ENABLE_ADDRESS_SANITIZER)
  set(EXTRA_FLAGS "${EXTRA_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
  # Some Linux distributions have apparently cannot pick the right linker
  # properly. See: http://stackoverflow.com/q/37603238/1170277
  if (BROKER_LINUX)
    set(EXTRA_FLAGS "${EXTRA_FLAGS} -fuse-ld=gold")
  endif ()
endif(ENABLE_ADDRESS_SANITIZER)

# Append our extra flags to the existing value of CXXFLAGS.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${EXTRA_FLAGS}")

install(DIRECTORY broker DESTINATION include FILES_MATCHING PATTERN "*.hh")

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR})
include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR}/3rdparty)

set_source_files_properties(3rdparty/sqlite3.c PROPERTIES COMPILE_FLAGS
                            -DSQLITE_OMIT_LOAD_EXTENSION)

include_directories(${CMAKE_CURRENT_BINARY_DIR})

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/broker/config.hh.in
               ${CMAKE_CURRENT_BINARY_DIR}/broker/config.hh)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/broker/config.hh DESTINATION include/broker)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/broker/version.hh.in
               ${CMAKE_CURRENT_BINARY_DIR}/broker/version.hh @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/broker/version.hh DESTINATION include/broker)

set(BROKER_SRC
  src/address.cc
  src/bro.cc
  src/configuration.cc
  src/data.cc
  src/endpoint.cc
  src/error.cc
  src/status_subscriber.cc
  src/internal_command.cc
  src/mailbox.cc
  src/network_info.cc
  src/peer_status.cc
  src/port.cc
  src/publisher.cc
  src/status.cc
  src/store.cc
  src/subnet.cc
  src/subscriber.cc
  src/time.cc
  src/topic.cc
  src/version.cc

  src/detail/abstract_backend.cc
  src/detail/clone_actor.cc
  src/detail/core_actor.cc
  src/detail/core_policy.cc
  src/detail/filesystem.cc
  src/detail/flare.cc
  src/detail/flare_actor.cc
  src/detail/make_backend.cc
  src/detail/master_actor.cc
  src/detail/master_resolver.cc
  src/detail/memory_backend.cc
  src/detail/network_cache.cc
  src/detail/prefix_matcher.cc
  src/detail/sqlite_backend.cc

  3rdparty/sqlite3.c

  ${OPTIONAL_SRC}
)

include(TestBigEndian)
test_big_endian(BROKER_BIG_ENDIAN)

include(CheckIncludeFiles)
set(CMAKE_REQUIRED_FLAGS -msse2)
check_include_files(emmintrin.h HAVE_SSE2)
set(CMAKE_REQUIRED_FLAGS)

if (HAVE_SSE2)
  add_definitions(-DBROKER_USE_SSE2 -msse2)
endif ()

if (NOT INSTALL_LIB_DIR)
  set(INSTALL_LIB_DIR ${CMAKE_INSTALL_PREFIX}/lib)
endif ()

if (ENABLE_SHARED)
  add_library(broker SHARED ${BROKER_SRC})
  set_target_properties(broker PROPERTIES
                        SOVERSION ${BROKER_SOVERSION}
                        VERSION ${BROKER_VERSION_MAJOR}.${BROKER_VERSION_MINOR}
                        MACOSX_RPATH true
                        OUTPUT_NAME broker)
  target_link_libraries(broker ${LINK_LIBS})
  install(TARGETS broker DESTINATION ${INSTALL_LIB_DIR})
endif ()

if (ENABLE_STATIC)
  add_library(broker_static STATIC ${BROKER_SRC})
  set_target_properties(broker_static PROPERTIES OUTPUT_NAME broker)
  target_link_libraries(broker_static ${LINK_LIBS})
  install(TARGETS broker_static DESTINATION ${INSTALL_LIB_DIR})
endif ()

# -- Tools --------------------------------------------------------------------

macro(add_tool name)
  add_executable(${name} src/${name}.cc ${ARGN})
  if (ENABLE_SHARED)
    target_link_libraries(${name} ${LINK_LIBS} broker)
    add_dependencies(${name} broker)
  else()
    target_link_libraries(${name} ${LINK_LIBS} broker_static)
    add_dependencies(${name} broker_static)
  endif()
endmacro()

add_tool(broker-pipe)

# -- Bindings -----------------------------------------------------------------

if (NOT DISABLE_PYTHON_BINDINGS)
  find_package(PythonInterp)
  if (NOT PYTHONINTERP_FOUND)
    message(STATUS "Skipping Python bindings: Python interpreter not found")
  endif ()

  if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/bindings/python/3rdparty/pybind11/CMakeLists.txt")
    message(WARNING "Skipping Python bindings: pybind11 submodule not available")
    set(PYTHONINTERP_FOUND false)
  endif ()

  if (${PYTHON_VERSION_MAJOR}.${PYTHON_VERSION_MINOR} VERSION_LESS 2.7)
    message(WARNING "Skipping Python bindings: Python 2.7 or Python 3 required")
    set(PYTHONINTERP_FOUND false)
  endif ()

  find_package(PythonDev)
  if (PYTHONDEV_FOUND)
    # The standard PythonLibs package puts its includes at PYTHON_INCLUDE_DIRS.
    set(PYTHON_INCLUDE_DIRS ${PYTHON_INCLUDE_DIR})
  else ()
    message(STATUS
            "Skipping Python bindings: Python includes/libraries not found")
  endif ()

  if (PYTHONINTERP_FOUND AND PYTHONDEV_FOUND)
    set (BROKER_PYTHON_BINDINGS true)
    set (BROKER_PYTHON_STAGING_DIR ${CMAKE_CURRENT_BINARY_DIR}/python)
    add_subdirectory(bindings/python)
  endif ()
endif ()

# -- Bro ----------------------------------------------------------------------


if (NOT "${BRO_EXECUTABLE}" STREQUAL "")
    set(BRO_FOUND true)
    set(BRO_FOUND_MSG "${BRO_EXECUTABLE}")
else ()
    set(BRO_FOUND false)
    find_file(BRO_PATH_DEV bro-path-dev.sh PATHS ${CMAKE_BINARY_DIR}/../../../build NO_DEFAULT_PATH)
    if (EXISTS ${BRO_PATH_DEV})
      set(BRO_FOUND true)
      set(BRO_FOUND_MSG "${BRO_PATH_DEV}")
    endif ()
endif ()

# -- Unit Tests ---------------------------------------------------------------

enable_testing()
add_subdirectory(tests)

# -- Documentation ------------------------------------------------------------

if (NOT BROKER_DISABLE_DOCS)
  add_subdirectory(doc)
endif ()

# -- Build Summary ------------------------------------------------------------

string(TOUPPER ${CMAKE_BUILD_TYPE} BuildType)

macro(display test desc summary)
  if ( ${test} )
    set(${summary} ${desc})
  else ()
    set(${summary} no)
  endif()
endmacro()

display(ENABLE_SHARED yes shared_summary)
display(ENABLE_STATIC yes static_summary)
display(CAF_FOUND "${caf_dir} (${CAF_VERSION})" caf_summary)
display(ROCKSDB_FOUND "${ROCKSDB_INCLUDE_DIRS}" rocksdb_summary)
display(BROKER_PYTHON_BINDINGS yes python_summary)
display(BRO_FOUND "${BRO_FOUND_MSG}" bro_summary)

set(summary
    "==================|  Broker Config Summary  |===================="
    "\nVersion:         ${BROKER_VERSION}"
    "\nSO version:      ${BROKER_SOVERSION}"
    "\n"
    "\nBuild Type:      ${CMAKE_BUILD_TYPE}"
    "\nInstall prefix:  ${CMAKE_INSTALL_PREFIX}"
    "\nLibrary prefix:  ${INSTALL_LIB_DIR}"
    "\nShared libs:     ${shared_summary}"
    "\nStatic libs:     ${static_summary}"
    "\n"
    "\nCC:              ${CMAKE_C_COMPILER}"
    "\nCFLAGS:          ${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${BuildType}}"
    "\nCXX:             ${CMAKE_CXX_COMPILER}"
    "\nCXXFLAGS:        ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${BuildType}}"
    "\n"
    "\nCAF:             ${caf_summary}"
    "\nRocksDB:         ${rocksdb_summary}"
    "\nPython bindings: ${python_summary}"
    "\nBro:             ${bro_summary}"
    "\n=================================================================")

message("\n" ${summary} "\n")
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/config.summary ${summary})

include(UserChangedWarning)
