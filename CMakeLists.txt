project(broker C CXX)
cmake_minimum_required(VERSION 2.8.12)
include(cmake/CommonCMakeConfig.cmake)

find_package(Libcaf COMPONENTS core io REQUIRED)
include_directories(BEFORE ${LIBCAF_INCLUDE_DIRS})

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

if ( ENABLE_STATIC_ONLY )
    set(ENABLE_SHARED false)
else ()
    set(ENABLE_SHARED true)
endif ()

if ( ENABLE_STATIC )
    set(ENABLE_STATIC true)
else ()
    set(ENABLE_STATIC false)
endif ()

set(CMAKE_CXX_FLAGS       "${CMAKE_CXX_FLAGS} -std=c++11")

install(DIRECTORY broker DESTINATION include)

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR})

# A convienience to add non-compiled files to IDEs - CMake doesn't add
# such files unless they appear in either add_executable or add_library.
file(GLOB_RECURSE BROKER_HDR *.h *.hh)

set(BROKER_SRC
    src/broker.cc
    src/endpoint.cc
    src/event_queue.cc
    src/log_queue.cc
    src/peering.cc
    src/peer_status_queue.cc
    src/print_queue.cc
    src/subscription.cc
    src/store/clone.cc
    src/store/frontend.cc
    src/store/master.cc
    src/store/response_queue.cc
    src/store/sequence_num.cc
    src/util/pipe.cc
    src/util/flare.cc
    ${BROKER_HDR})

add_library(broker_objs OBJECT ${BROKER_SRC})

if ( ENABLE_SHARED )
    add_library(broker SHARED $<TARGET_OBJECTS:broker_objs>)
    set_target_properties(broker PROPERTIES
                          SOVERSION ${BROKER_SOVERSION}
                          VERSION ${BROKER_VERSION}
                          MACOSX_RPATH true
                          OUTPUT_NAME broker)
    target_link_libraries(broker ${LIBCAF_LIBRARIES})
    install(TARGETS broker DESTINATION ${INSTALL_LIB_DIR})
endif ()

if ( ENABLE_STATIC )
    add_library(brokerStatic STATIC $<TARGET_OBJECTS:broker_objs>)
    set_target_properties(brokerStatic PROPERTIES OUTPUT_NAME broker)
    target_link_libraries(brokerStatic ${LIBCAF_LIBRARIES})
    install(TARGETS brokerStatic DESTINATION ${INSTALL_LIB_DIR})
endif ()

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
    "\n"
    "\nCC:              ${CMAKE_C_COMPILER}"
    "\nCFLAGS:          ${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${BuildType}}"
    "\nCXX:             ${CMAKE_CXX_COMPILER}"
    "\nCXXFLAGS:        ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${BuildType}}"
    "\n=================================================================")

include(UserChangedWarning)
