# -- Project Setup ------------------------------------------------------------

project(broker C CXX)
cmake_minimum_required(VERSION 2.8)
include(cmake/CommonCMakeConfig.cmake)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR}/lib)

# -- Dependencies -------------------------------------------------------------

# Extract CAF version.
find_package(CAF COMPONENTS core io test REQUIRED)
file(READ "${CAF_INCLUDE_DIR_CORE}/caf/config.hpp" caf_config_hpp)
string(REGEX MATCH "#define CAF_VERSION [0-9]+"
       caf_version_line "${caf_config_hpp}")
string(REGEX MATCH "[0-9]+" caf_version_int "${caf_version_line}")
math(EXPR caf_version_major "${caf_version_int} / 10000")
math(EXPR caf_version_minor "( ${caf_version_int} / 100) % 100")
math(EXPR caf_version_patch "${caf_version_int} % 100")
set(CAF_VERSION
    "${caf_version_major}.${caf_version_minor}.${caf_version_patch}")

# Check for required CAF version.
set(CAF_VERSION_REQUIRED 0.15.2)
if (CAF_VERSION VERSION_LESS ${CAF_VERSION_REQUIRED})
  message(FATAL_ERROR "Broker requires at least CAF version"
    " ${CAF_VERSION_REQUIRED}, detected version: ${CAF_VERSION}")
endif ()

include_directories(BEFORE ${CAF_INCLUDE_DIRS})
set(LINK_LIBS ${LINK_LIBS} ${CAF_LIBRARIES})

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

install(DIRECTORY broker DESTINATION include)

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR})

set_source_files_properties(src/detail/sqlite3.c PROPERTIES COMPILE_FLAGS
                            -DSQLITE_OMIT_LOAD_EXTENSION)

include_directories(${CMAKE_CURRENT_BINARY_DIR})

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/broker/config.hh.in
               ${CMAKE_CURRENT_BINARY_DIR}/broker/config.hh)

set(BROKER_SRC
  src/address.cc
  src/configuration.cc
  src/data.cc
  src/endpoint.cc
  src/error.cc
  src/event_subscriber.cc
  src/internal_command.cc
  src/mailbox.cc
  src/network_info.cc
  src/port.cc
  src/publisher.cc
  src/status.cc
  src/store.cc
  src/stream_governor.cc
  src/stream_relay.cc
  src/subnet.cc
  src/subscriber.cc
  src/time.cc
  src/topic.cc
  src/version.cc

  src/detail/abstract_backend.cc
  src/detail/clone_actor.cc
  src/detail/core_actor.cc
  src/detail/filesystem.cc
  src/detail/flare.cc
  src/detail/flare_actor.cc
  src/detail/make_backend.cc
  src/detail/master_actor.cc
  src/detail/master_resolver.cc
  src/detail/memory_backend.cc
  src/detail/network_cache.cc
  src/detail/sqlite_backend.cc

  src/detail/sqlite3.c
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
                        # INSTALL_NAME_DIR can be removed when
                        # CMake 2.8.12 is required (redundant with
                        # MACOSX_RPATH).
                        INSTALL_NAME_DIR "@rpath"
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

# -- Bindings -----------------------------------------------------------------

if (NOT DISABLE_PYTHON_BINDINGS)
  find_package(PythonInterp)
  if (NOT PYTHONINTERP_FOUND)
    message(STATUS "Skipping Python bindings: Python interpreter not found")
  endif ()

  find_package(PythonDev)
  if (PYTHONDEV_FOUND)
    # The standard PythonLibs package puts its includes at PYTHON_INCLUDE_DIRS.
    set(PYTHON_INCLUDE_DIRS ${PYTHON_INCLUDE_DIR})
  else ()
    message(STATUS
            "Skipping Python bindings: Python includes/libraries not found")
  endif ()

  find_package(Pybind11 QUIET)
  if (NOT PYBIND11_FOUND)
    message(STATUS "Skipping Python bindings: pybind11 not found")
  endif ()

  if (PYTHONINTERP_FOUND AND PYTHONDEV_FOUND AND PYBIND11_FOUND)
    set (BROKER_PYTHON_BINDINGS true)
    set (BROKER_PYTHON_STAGING_DIR ${CMAKE_CURRENT_BINARY_DIR}/python)
    add_subdirectory(bindings/python)
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

set(caf_dir ${CAF_LIBRARY_CORE})

get_filename_component(caf_dir ${caf_dir} PATH)
if (EXISTS "${caf_dir}/../libcaf_core")
  get_filename_component(caf_dir ${caf_dir} PATH)
else ()
  set(caf_dir ${CAF_INCLUDE_DIR_CORE})
endif ()

display(ENABLE_SHARED yes shared_summary)
display(ENABLE_STATIC yes static_summary)
display(CAF_FOUND "${caf_dir} (${CAF_VERSION})" caf_summary)
display(ROCKSDB_FOUND "${ROCKSDB_INCLUDE_DIRS}" rocksdb_summary)
display(BROKER_PYTHON_BINDINGS "${PYBIND11_INCLUDE_DIRS}" python_summary)

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
    "\n=================================================================")

message("\n" ${summary} "\n")
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/config.summary ${summary})

include(UserChangedWarning)
