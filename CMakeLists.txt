project(broker C CXX)
cmake_minimum_required(VERSION 2.8)
include(cmake/CommonCMakeConfig.cmake)

find_package(Libcaf COMPONENTS core io REQUIRED)

# Check for required CAF version.
set(CAF_VERSION_REQUIRED 0.13)
file(READ "${LIBCAF_INCLUDE_DIR_CORE}/caf/config.hpp" caf_config_hpp)
string(REGEX MATCH "#define CAF_VERSION [0-9]+"
       caf_version_line "${caf_config_hpp}")
string(REGEX MATCH "[0-9]+" caf_version_int "${caf_version_line}")
math(EXPR caf_version_major "${caf_version_int} / 10000")
math(EXPR caf_version_minor "( ${caf_version_int} / 100) % 100")
math(EXPR caf_version_patch "${caf_version_int} % 100")
set(CAF_VERSION
    "${caf_version_major}.${caf_version_minor}.${caf_version_patch}")

if ( CAF_VERSION VERSION_LESS ${CAF_VERSION_REQUIRED} )
    message(FATAL_ERROR "Broker requires at least CAF version"
            " ${CAF_VERSION_REQUIRED}, detected version: ${CAF_VERSION}")
endif ()

message(STATUS "Found CAF version: ${CAF_VERSION}")
include_directories(BEFORE ${LIBCAF_INCLUDE_DIRS})
set(LINK_LIBS ${LINK_LIBS} ${LIBCAF_LIBRARIES})

find_package(RocksDB)

if ( ROCKSDB_FOUND )
    include_directories(BEFORE ${ROCKSDB_INCLUDE_DIRS})
    set(LINK_LIBS ${LINK_LIBS} ${ROCKSDB_LIBRARIES})
    set(OPTIONAL_SRC ${OPTIONAL_SRC} src/store/rocksdb_backend.cc)
endif ()

file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/VERSION" VERSION LIMIT_COUNT 1)
string(REPLACE "." " " _version_numbers ${VERSION})
separate_arguments(_version_numbers)
list(GET _version_numbers 0 BROKER_VERSION_MAJOR)
list(GET _version_numbers 1 BROKER_VERSION_MINOR)
list(LENGTH _version_numbers _version_length)

if ( _version_length LESS 3 )
    set(BROKER_VERSION_PATCH 0)
else ()
    list(GET _version_numbers 2 BROKER_VERSION_PATCH)
endif ()

set(BROKER_VERSION
    ${BROKER_VERSION_MAJOR}.${BROKER_VERSION_MINOR}.${BROKER_VERSION_PATCH})
# The SO number shall increase only if binary interface changes.
set(BROKER_SOVERSION 0)
set(ENABLE_SHARED true)

if ( ENABLE_STATIC_ONLY )
    set(ENABLE_STATIC true)
    set(ENABLE_SHARED false)
endif ()

include(RequireCXX11)

install(DIRECTORY broker DESTINATION include)

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR})

# A convienience to add non-compiled files to IDEs - CMake doesn't add
# such files unless they appear in either add_executable or add_library.
file(GLOB_RECURSE BROKER_HDR *.h *.hh)

set_source_files_properties(src/store/sqlite3.c PROPERTIES COMPILE_FLAGS
                            -DSQLITE_OMIT_LOAD_EXTENSION)

set(BROKER_SRC
    src/address.cc
    src/broker.cc
    src/data.cc
    src/endpoint.cc
    src/enum_value.cc
    src/incoming_connection_status.cc
    src/message_queue.cc
    src/outgoing_connection_status.cc
    src/peering.cc
    src/persistables.cc
    src/port.cc
    src/queue.cc
    src/record.cc
    src/report.cc
    src/set.cc
    src/string.cc
    src/subnet.cc
    src/subscription.cc
    src/table.cc
    src/time_duration.cc
    src/time_point.cc
    src/vector.cc

    src/store/backend.cc
    src/store/clone.cc
    src/store/expiration_time.cc
    src/store/frontend.cc
    src/store/master.cc
    src/store/memory_backend.cc
    src/store/query.cc
    src/store/response.cc
    src/store/result.cc
    src/store/sequence_num.cc
    src/store/sqlite3.c
    src/store/sqlite_backend.cc

    src/util/misc.cc
    src/util/persist.cc
    src/util/pipe.cc
    src/util/flare.cc

    ${OPTIONAL_SRC}
    ${BROKER_HDR})

include(TestBigEndian)
test_big_endian(BROKER_BIG_ENDIAN)

if ( BROKER_BIG_ENDIAN )
    set_source_files_properties(src/util/persist.c PROPERTIES COMPILE_FLAGS
                                -DBROKER_BIG_ENDIAN)
endif ()

if ( NOT INSTALL_LIB_DIR )
    set(INSTALL_LIB_DIR ${CMAKE_INSTALL_PREFIX}/lib)
endif ()

if ( ENABLE_SHARED )
    add_library(broker SHARED ${BROKER_SRC})
    set_target_properties(broker PROPERTIES
                          SOVERSION ${BROKER_SOVERSION}
                          VERSION ${BROKER_VERSION}
                          MACOSX_RPATH true
                          # INSTALL_NAME_DIR can be removed when
                          # CMake 2.8.12 is required (redundant with
                          # MACOSX_RPATH).
                          INSTALL_NAME_DIR "@rpath"
                          OUTPUT_NAME broker)
    target_link_libraries(broker ${LINK_LIBS})
    install(TARGETS broker DESTINATION ${INSTALL_LIB_DIR})
endif ()

if ( ENABLE_STATIC )
    add_library(brokerStatic STATIC ${BROKER_SRC})
    set_target_properties(brokerStatic PROPERTIES OUTPUT_NAME broker)
    target_link_libraries(brokerStatic ${LINK_LIBS})
    install(TARGETS brokerStatic DESTINATION ${INSTALL_LIB_DIR})
endif ()

add_subdirectory(bindings)

enable_testing()
add_subdirectory(tests)

string(TOUPPER ${CMAKE_BUILD_TYPE} BuildType)

message(
    "\n==================|  broker Config Summary  |===================="
    "\nVersion:         ${BROKER_VERSION}"
    "\nSO version:      ${BROKER_SOVERSION}"
    "\nBuild Type:      ${CMAKE_BUILD_TYPE}"
    "\nInstall prefix:  ${CMAKE_INSTALL_PREFIX}"
    "\nLibrary prefix:  ${INSTALL_LIB_DIR}"
    "\nShared libs:     ${ENABLE_SHARED}"
    "\nStatic libs:     ${ENABLE_STATIC}"
    "\nEnable RocksDB:  ${ROCKSDB_FOUND}"
    "\nPython bindings: ${BROKER_PYTHON_BINDINGS}"
    "\n"
    "\nCC:              ${CMAKE_C_COMPILER}"
    "\nCFLAGS:          ${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${BuildType}}"
    "\nCXX:             ${CMAKE_CXX_COMPILER}"
    "\nCXXFLAGS:        ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${BuildType}}"
    "\n=================================================================")

include(UserChangedWarning)
