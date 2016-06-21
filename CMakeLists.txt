# -- Project Setup ------------------------------------------------------------

project(broker C CXX)
cmake_minimum_required(VERSION 2.8)
include(cmake/CommonCMakeConfig.cmake)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR}/bin
  CACHE PATH "Single directory for all executables")

set (LIBRARY_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR}/lib
  CACHE PATH "Single directory for all libraries")

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
set(CAF_VERSION_REQUIRED 0.14)
if (CAF_VERSION VERSION_LESS ${CAF_VERSION_REQUIRED})
  message(FATAL_ERROR "Broker requires at least CAF version"
    " ${CAF_VERSION_REQUIRED}, detected version: ${CAF_VERSION}")
endif ()

include_directories(BEFORE ${CAF_INCLUDE_DIRS})
set(LINK_LIBS ${LINK_LIBS} ${CAF_LIBRARIES})

# RocksDB
find_package(RocksDB)
if (ROCKSDB_FOUND)
  include_directories(BEFORE ${ROCKSDB_INCLUDE_DIRS})
  set(LINK_LIBS ${LINK_LIBS} ${ROCKSDB_LIBRARIES})
  set(OPTIONAL_SRC ${OPTIONAL_SRC} src/store/rocksdb_backend.cc)
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

include(RequireCXX11)

# Increase maximum number of instantiations.
set(EXTRA_FLAGS "${EXTRA_FLAGS} -ftemplate-depth=512")

# Reduce the number of template instantiations shown in backtrace.
set(EXTRA_FLAGS "${EXTRA_FLAGS} -ftemplate-backtrace-limit=3")

if (ENABLE_ADDRESS_SANITIZER)
  set(EXTRA_FLAGS "${EXTRA_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
endif(ENABLE_ADDRESS_SANITIZER)

# Append our extra flags to the existing value of CXXFLAGS.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${EXTRA_FLAGS}")

install(DIRECTORY broker DESTINATION include)

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR})

set_source_files_properties(src/store/sqlite3.c PROPERTIES COMPILE_FLAGS
                            -DSQLITE_OMIT_LOAD_EXTENSION)

set(BROKER_SRC
  src/address.cc
  src/configuration.cc
  src/context.cc
  src/data.cc
  src/endpoint.cc
  src/enum_value.cc
  src/error.cc
  #src/persistables.cc
  src/network_info.cc
  src/port.cc
  src/record.cc
  src/set.cc
  src/status.cc
  src/string.cc
  src/subnet.cc
  src/table.cc
  src/topic.cc
  src/time_duration.cc
  src/time_point.cc
  #src/vector.cc

  src/detail/pipe.cc
  src/detail/flare.cc
  src/detail/scoped_flare_actor.cc
  #src/detail/subscription.cc

  #src/store/backend.cc
  #src/store/clone.cc
  #src/store/expiration_time.cc
  #src/store/frontend.cc
  #src/store/master.cc
  #src/store/memory_backend.cc
  #src/store/query.cc
  #src/store/response.cc
  #src/store/result.cc
  #src/store/sequence_num.cc
  #src/store/sqlite3.c
  #src/store/sqlite_backend.cc

  src/util/misc.cc
  src/util/persist.cc

  #${OPTIONAL_SRC}
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

if (BROKER_BIG_ENDIAN)
  set_source_files_properties(src/util/persist.c PROPERTIES COMPILE_FLAGS
                              -DBROKER_BIG_ENDIAN)
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

add_subdirectory(tests)

# -- Bindings ----------------------------------------------------------------

# FIXME
#add_subdirectory(bindings)

string(TOUPPER ${CMAKE_BUILD_TYPE} BuildType)

# -- Build Summary ------------------------------------------------------------

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
display(ROCKSDB_FOUND ${ROCKSDB_INCLUDE_DIRS} rocksdb_summary)
display(BROKER_PYTHON_BINDINGS ${BROKER_PYTHON_BINDINGS} pybindings_summary)

message(
    "\n==================|  broker Config Summary  |===================="
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
    "\nPython bindings: ${pybindings_summary}"
    "\n=================================================================")

include(UserChangedWarning)
