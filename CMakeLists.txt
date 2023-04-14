# -- Project Setup ------------------------------------------------------------

cmake_minimum_required(VERSION 3.15 FATAL_ERROR)
project(broker C CXX)
include(CMakePackageConfigHelpers)
include(GNUInstallDirs)
include(cmake/CommonCMakeConfig.cmake)

get_directory_property(parent_dir PARENT_DIRECTORY)
if(parent_dir)
  set(broker_is_subproject ON)
else()
  set(broker_is_subproject OFF)
endif()
unset(parent_dir)

if ( MSVC )
  message(STATUS "Broker currently only supports static bulids on Windows")
  message(STATUS "Note: continue with ENABLE_STATIC_ONLY")
  set(ENABLE_STATIC_ONLY ON)
endif ()

# Check the thread library info early as setting compiler flags seems to
# interfere with the detection and causes CMAKE_THREAD_LIBS_INIT to not
# include -lpthread when it should.
if (NOT TARGET Threads::Threads)
  find_package(Threads REQUIRED)
endif ()
set(LINK_LIBS ${LINK_LIBS} Threads::Threads)

# Leave most compiler flags alone when building as subdirectory.
if (NOT broker_is_subproject)

  set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/bin)
  set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/lib)
  set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/lib)

  if ( ENABLE_CCACHE )
    find_program(CCACHE_PROGRAM ccache)

    if ( NOT CCACHE_PROGRAM )
      message(FATAL_ERROR "ccache not found")
    endif ()

    message(STATUS "Using ccache: ${CCACHE_PROGRAM}")
    set(CMAKE_C_COMPILER_LAUNCHER   ${CCACHE_PROGRAM})
    set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE_PROGRAM})
  endif ()

  if ( BROKER_SANITIZERS )
      set(_sanitizer_flags "-fsanitize=${BROKER_SANITIZERS}")
      set(_sanitizer_flags "${_sanitizer_flags} -fno-omit-frame-pointer")
      set(_sanitizer_flags "${_sanitizer_flags} -fno-optimize-sibling-calls")

      if ( NOT DEFINED BROKER_SANITIZER_OPTIMIZATIONS )
        if ( DEFINED ENV{NO_OPTIMIZATIONS} )
          # Using -O1 is generally the suggestion to get more reasonable
          # performance.  The one downside is it that the compiler may optimize
          # out code that otherwise generates an error/leak in a -O0 build, but
          # that should be rare and users mostly will not be running unoptimized
          # builds in production anyway.
          set(BROKER_SANITIZER_OPTIMIZATIONS false CACHE INTERNAL "" FORCE)
        else ()
          set(BROKER_SANITIZER_OPTIMIZATIONS true CACHE INTERNAL "" FORCE)
        endif ()
      endif ()

      if ( BROKER_SANITIZER_OPTIMIZATIONS )
        set(_sanitizer_flags "${_sanitizer_flags} -O1")
      endif ()

      # Technically, then we also need to use the compiler to drive linking and
      # give the sanitizer flags there, too.  However, CMake, by default, uses
      # the compiler for linking and so the flags automatically get used.  See
      # https://cmake.org/pipermail/cmake/2014-August/058268.html
      set(CAF_EXTRA_FLAGS "${_sanitizer_flags}")

      # Set EXTRA_FLAGS if broker isn't being built as part of a Zeek build.
      # The Zeek build sets it otherwise.
      if ( NOT ZEEK_SANITIZERS )
        set(EXTRA_FLAGS "${EXTRA_FLAGS} ${_sanitizer_flags}")
      endif ()
  endif()

endif()

if ( MSVC )
    # Allow more sections in object files, otherwise Broker fails to compile.
    set(EXTRA_FLAGS "${EXTRA_FLAGS} /bigobj")
else ()
    # Increase warnings.
    set(EXTRA_FLAGS "${EXTRA_FLAGS} -Wall -Wno-unused -pedantic")
    # Increase maximum number of instantiations.
    set(EXTRA_FLAGS "${EXTRA_FLAGS} -ftemplate-depth=512")
    # Make sure to get the full context on template errors.
    set(EXTRA_FLAGS "${EXTRA_FLAGS} -ftemplate-backtrace-limit=0")
endif ()

# Append our extra flags to the existing value of CXXFLAGS.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${EXTRA_FLAGS}")

include(RequireCXX17)
include(CheckCXXSourceCompiles)

# Unfortunately, std::filesystem is a mess. The feature checks at compile time
# via __has_include(<filesystem>) or __cpp_lib_filesystem only tell half of the
# story. On some older Clang releases, one also needs to additional link to
# -lc++fs. On some older GCC releases, -lstdc++-fs is required. We do have a
# fallback for pre-std-filesystem on POSIX systems. So, instead of doing all the
# flag guessing, we simply check once whether code using <filesystem> compiles,
# links and runs. Otherwise, we fall back to our pre-std-filesystem
# implementation - or raise an error when building on Windows (since we don't
# have a fallback on this platform).
check_cxx_source_compiles("
  #include <cstdlib>
  #include <filesystem>
  int main(int, char**) {
    auto cwd = std::filesystem::current_path();
    auto str = cwd.string();
    return str.empty() ? EXIT_FAILURE : EXIT_SUCCESS;
  }
  "
  BROKER_HAS_STD_FILESYSTEM)

if (MSVC)

  if (NOT BROKER_HAS_STD_FILESYSTEM)
    message(FATAL_ERROR "No std::filesystem support! Required on MSVC.")
  endif ()

  set(BROKER_USE_SSE2 OFF)

elseif (NOT BROKER_DISABLE_SSE2_CHECK)

  include(CheckIncludeFiles)
  set(CMAKE_REQUIRED_FLAGS -msse2)
  check_include_files(emmintrin.h BROKER_USE_SSE2)
  set(CMAKE_REQUIRED_FLAGS)

  if (BROKER_USE_SSE2)
    add_definitions(-msse2)
  endif ()

endif ()

if (NOT BROKER_DISABLE_ATOMICS_CHECK)

  set(atomic_64bit_ops_test "
    #include <atomic>
    struct s64 { char a, b, c, d, e, f, g, h; };
    int main() { std::atomic<s64> x; x.store({}); x.load(); return 0; }
  ")
  check_cxx_source_compiles("${atomic_64bit_ops_test}" atomic64_builtin)

  if ( NOT atomic64_builtin )
    set(CMAKE_REQUIRED_LIBRARIES atomic)
    check_cxx_source_compiles("${atomic_64bit_ops_test}" atomic64_with_lib)
    set(CMAKE_REQUIRED_LIBRARIES)

    if ( atomic64_with_lib )
      set(LINK_LIBS ${LINK_LIBS} atomic)
    else ()
      # Guess we'll find out for sure when we compile/link.
      message(WARNING "build may fail due to missing 64-bit atomic support")
    endif ()
  endif ()

endif ()

# -- Platform Setup ----------------------------------------------------------

if (APPLE)
  set(BROKER_APPLE true)
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(BROKER_LINUX true)
elseif(${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
  set(BROKER_FREEBSD true)
elseif(WIN32)
  set(BROKER_WINDOWS true)
endif ()

include(TestBigEndian)
test_big_endian(BROKER_BIG_ENDIAN)

# -- Dependencies -------------------------------------------------------------

if (WIN32)
  set(LINK_LIBS ${LINK_LIBS} ws2_32 iphlpapi crypt32)
endif ()

# Search for OpenSSL if not already provided by parent project
if (NOT OPENSSL_LIBRARIES)
  find_package(OpenSSL REQUIRED)
  include_directories(BEFORE ${OPENSSL_INCLUDE_DIR})
endif()
set(LINK_LIBS ${LINK_LIBS} OpenSSL::SSL OpenSSL::Crypto)

function(add_bundled_caf)
  # Disable unnecessary features and make sure CAF builds static libraries.
  set(CAF_ENABLE_EXAMPLES OFF)
  set(CAF_ENABLE_TESTING OFF)
  set(CAF_ENABLE_TOOLS OFF)
  set(BUILD_SHARED_LIBS OFF)
  add_subdirectory(caf EXCLUDE_FROM_ALL)
endfunction()

# NOTE: building and linking against an external CAF version is NOT supported!
#       This variable is FOR DEVELOPMENT ONLY. The only officially supported CAF
#       version is the bundled version!
if (CAF_ROOT)
  message(STATUS "Using system CAF version ${CAF_VERSION}")
  # TODO: drop < 3.12 compatibility check when raising the minimum CMake version
  if (CMAKE_VERSION VERSION_LESS 3.12)
    find_package(CAF REQUIRED
                 COMPONENTS openssl test io core
                 PATHS "${CAF_ROOT}")
  else()
    find_package(CAF REQUIRED COMPONENTS openssl test io core net)
  endif()
  list(APPEND LINK_LIBS CAF::core CAF::io CAF::net)
  set(BROKER_USE_EXTERNAL_CAF ON)
else ()
  # Note: we use the object libraries here to avoid having dependencies on the
  # actual CAF targets that would force consumers of Broker to have CMake being
  # able to find a CAF installation.
  message(STATUS "Using bundled CAF")
  add_bundled_caf()
  list(APPEND OPTIONAL_SRC $<TARGET_OBJECTS:libcaf_core_obj>)
  list(APPEND OPTIONAL_SRC $<TARGET_OBJECTS:libcaf_io_obj>)
  list(APPEND OPTIONAL_SRC $<TARGET_OBJECTS:libcaf_net_obj>)
  set(BROKER_USE_EXTERNAL_CAF OFF)
endif ()

# -- libroker -----------------------------------------------------------------

file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/VERSION" BROKER_VERSION LIMIT_COUNT 1)
string(REPLACE "." " " _version_numbers ${BROKER_VERSION})
separate_arguments(_version_numbers)
list(GET _version_numbers 0 BROKER_VERSION_MAJOR)
list(GET _version_numbers 1 BROKER_VERSION_MINOR)

# The SO number shall increase only if binary interface changes.
set(BROKER_SOVERSION 4)
set(ENABLE_SHARED true)

if (ENABLE_STATIC_ONLY)
  set(ENABLE_STATIC true)
  set(ENABLE_SHARED false)
endif ()

# Make sure there are no old header versions on disk.
install(
  CODE "MESSAGE(STATUS \"Removing: ${CMAKE_INSTALL_PREFIX}/include/broker\")"
  CODE "file(REMOVE_RECURSE \"${CMAKE_INSTALL_PREFIX}/include/broker\")")

# Install all headers except the files from broker/internal.
install(DIRECTORY include/broker
        DESTINATION include
        FILES_MATCHING PATTERN "*.hh"
                       PATTERN "include/broker/internal" EXCLUDE)

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR}/include)

include_directories(${CMAKE_CURRENT_BINARY_DIR}/include)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/src/config.hh.in
               ${CMAKE_CURRENT_BINARY_DIR}/include/broker/config.hh)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/include/broker/config.hh DESTINATION include/broker)

if (NOT BROKER_EXTERNAL_SQLITE_TARGET)
  include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR}/3rdparty)
  set_source_files_properties(3rdparty/sqlite3.c PROPERTIES COMPILE_FLAGS
                              -DSQLITE_OMIT_LOAD_EXTENSION)
  list(APPEND OPTIONAL_SRC 3rdparty/sqlite3.c)
else()
  list(APPEND LINK_LIBS ${BROKER_EXTERNAL_SQLITE_TARGET})
endif()

set(BROKER_SRC
  # src/detail/core_recorder.cc
  # src/detail/generator_file_reader.cc
  # src/detail/generator_file_writer.cc
  # src/gateway.cc
  # src/internal/data_generator.cc
  # src/internal/generator_file_reader.cc
  # src/internal/generator_file_writer.cc
  # src/internal/meta_command_writer.cc
  # src/internal/meta_data_writer.cc
  ${OPTIONAL_SRC}
  src/address.cc
  src/alm/multipath.cc
  src/alm/routing_table.cc
  src/configuration.cc
  src/convert.cc
  src/data.cc
  src/detail/abstract_backend.cc
  src/detail/filesystem.cc
  src/detail/flare.cc
  src/detail/make_backend.cc
  src/detail/memory_backend.cc
  src/detail/monotonic_buffer_resource.cc
  src/detail/opaque_type.cc
  src/detail/peer_status_map.cc
  src/detail/prefix_matcher.cc
  src/detail/sink_driver.cc
  src/detail/source_driver.cc
  src/detail/sqlite_backend.cc
  src/detail/store_state.cc
  src/domain_options.cc
  src/endpoint.cc
  src/endpoint_id.cc
  src/endpoint_info.cc
  src/entity_id.cc
  src/error.cc
  src/filter_type.cc
  src/internal/clone_actor.cc
  src/internal/connector.cc
  src/internal/connector_adapter.cc
  src/internal/core_actor.cc
  src/internal/flare_actor.cc
  src/internal/json_client.cc
  src/internal/json_type_mapper.cc
  src/internal/master_actor.cc
  src/internal/master_resolver.cc
  src/internal/metric_collector.cc
  src/internal/metric_exporter.cc
  src/internal/metric_factory.cc
  src/internal/metric_scraper.cc
  src/internal/metric_view.cc
  src/internal/peering.cc
  src/internal/pending_connection.cc
  src/internal/prometheus.cc
  src/internal/store_actor.cc
  src/internal/web_socket.cc
  src/internal/wire_format.cc
  src/internal_command.cc
  src/mailbox.cc
  src/message.cc
  src/network_info.cc
  src/peer_status.cc
  src/port.cc
  src/publisher.cc
  src/shutdown_options.cc
  src/status.cc
  src/status_subscriber.cc
  src/store.cc
  src/store_event.cc
  src/subnet.cc
  src/subscriber.cc
  src/telemetry/counter.cc
  src/telemetry/gauge.cc
  src/telemetry/histogram.cc
  src/telemetry/metric_family.cc
  src/telemetry/metric_registry.cc
  src/telemetry/metric_registry_impl.cc
  src/time.cc
  src/topic.cc
  src/version.cc
  src/worker.cc
)

if (ENABLE_SHARED)
  add_library(broker SHARED ${BROKER_SRC})
  set_target_properties(broker PROPERTIES
                        SOVERSION ${BROKER_SOVERSION}
                        VERSION ${BROKER_VERSION_MAJOR}.${BROKER_VERSION_MINOR}
                        MACOSX_RPATH true
                        OUTPUT_NAME broker)
  target_link_libraries(broker PUBLIC ${LINK_LIBS})
  target_link_libraries(broker PRIVATE CAF::core CAF::io CAF::net)
  install(TARGETS broker
          EXPORT BrokerTargets
          DESTINATION ${CMAKE_INSTALL_LIBDIR})
  target_include_directories(broker INTERFACE
                             $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                             $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>
                             $<INSTALL_INTERFACE:include>)
endif ()

if (ENABLE_STATIC)
  add_library(broker_static STATIC ${BROKER_SRC})
  set_target_properties(broker_static PROPERTIES OUTPUT_NAME broker)
  if (NOT DISABLE_PYTHON_BINDINGS)
    set_target_properties(broker_static PROPERTIES POSITION_INDEPENDENT_CODE ON)
  endif()
  target_link_libraries(broker_static PUBLIC ${LINK_LIBS})
  target_link_libraries(broker_static PRIVATE CAF::core CAF::io CAF::net)
  install(TARGETS broker_static
          EXPORT BrokerTargets
          DESTINATION ${CMAKE_INSTALL_LIBDIR})
  target_include_directories(broker_static INTERFACE
                             $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                             $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>
                             $<INSTALL_INTERFACE:include>)
endif ()

if (ENABLE_SHARED)
  set(BROKER_LIBRARY broker)
else()
  set(BROKER_LIBRARY broker_static)
endif()

set(tidyCfgFile "${CMAKE_SOURCE_DIR}/.clang-tidy")
if (BROKER_ENABLE_TIDY)
  # Create a preprocessor definition that depends on .clang-tidy content so
  # the compile command will change when .clang-tidy changes. This ensures
  # that a subsequent build re-runs clang-tidy on all sources even if they
  # do not otherwise need to be recompiled.
  file(SHA1 ${CMAKE_CURRENT_SOURCE_DIR}/.clang-tidy clang_tidy_sha1)
  set(BROKER_CLANG_TIDY_DEF "CLANG_TIDY_SHA1=${clang_tidy_sha1}")
  unset(clang_tidy_sha1)
  add_custom_target(clang_tidy_dummy DEPENDS ${tidyCfgFile})
  set_target_properties(${BROKER_LIBRARY} PROPERTIES
                        CXX_CLANG_TIDY "clang-tidy;--config-file=${tidyCfgFile}")
  target_compile_definitions(${BROKER_LIBRARY} PRIVATE ${BROKER_CLANG_TIDY_DEF})
  configure_file(.clang-tidy .clang-tidy COPYONLY)
endif ()


# -- Tools --------------------------------------------------------------------

macro(add_tool name)
  add_executable(${name} src/${name}.cc ${ARGN})
  if (ENABLE_SHARED)
    target_link_libraries(${name} broker CAF::core CAF::io CAF::net)
    add_dependencies(${name} broker)
  else()
    target_link_libraries(${name} broker_static CAF::core CAF::io CAF::net)
    add_dependencies(${name} broker_static)
  endif()
  if (BROKER_ENABLE_TIDY)
    set_target_properties(${name} PROPERTIES
                          CXX_CLANG_TIDY "clang-tidy;--config-file=${tidyCfgFile}")
    target_compile_definitions(${name} PRIVATE ${BROKER_CLANG_TIDY_DEF})
  endif ()
endmacro()

if (NOT BROKER_DISABLE_TOOLS)
  # TODO: fix these tools
  # add_tool(broker-gateway)
  add_tool(broker-node)
  add_tool(broker-pipe)
endif ()

# -- Bindings -----------------------------------------------------------------

if (NOT DISABLE_PYTHON_BINDINGS)
  set(Python_ADDITIONAL_VERSIONS 3)
  find_package(PythonInterp)
  if (NOT PYTHONINTERP_FOUND)
    message(STATUS "Skipping Python bindings: Python interpreter not found")
  endif ()

  if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/bindings/python/3rdparty/pybind11/CMakeLists.txt")
    message(WARNING "Skipping Python bindings: pybind11 submodule not available")
    set(PYTHONINTERP_FOUND false)
  endif ()

  if (${PYTHON_VERSION_MAJOR}.${PYTHON_VERSION_MINOR} VERSION_LESS 3.5)
    message(WARNING "Skipping Python bindings: Python 3.5 or greater required")
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

# -- Zeek ---------------------------------------------------------------------

if (NOT "${ZEEK_EXECUTABLE}" STREQUAL "")
    set(ZEEK_FOUND true)
    set(ZEEK_FOUND_MSG "${ZEEK_EXECUTABLE}")
else ()
    set(ZEEK_FOUND false)
    find_file(ZEEK_PATH_DEV zeek-path-dev.sh PATHS ${CMAKE_CURRENT_BINARY_DIR}/../../../build NO_DEFAULT_PATH)
    if (EXISTS ${ZEEK_PATH_DEV})
      set(ZEEK_FOUND true)
      set(ZEEK_FOUND_MSG "${ZEEK_PATH_DEV}")
    endif ()
endif ()

# -- Unit Tests ---------------------------------------------------------------

if ( NOT BROKER_DISABLE_TESTS )
  enable_testing()
  add_subdirectory(tests)
endif ()

# -- Documentation ------------------------------------------------------------

if (NOT WIN32 AND NOT BROKER_DISABLE_DOCS)
  add_subdirectory(doc)
endif ()

# -- CMake package install ----------------------------------------------------

export(EXPORT BrokerTargets FILE BrokerTargets.cmake)

install(
  EXPORT BrokerTargets
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/Broker")

configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/src/BrokerConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/BrokerConfig.cmake"
  INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/Broker")

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/BrokerConfigVersion.cmake"
  VERSION ${BROKER_VERSION}
  COMPATIBILITY ExactVersion)

install(
  FILES
    "${CMAKE_CURRENT_BINARY_DIR}/BrokerConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/BrokerConfigVersion.cmake"
  DESTINATION
    "${CMAKE_INSTALL_LIBDIR}/cmake/Broker")

# -- Build Summary ------------------------------------------------------------

if (CMAKE_BUILD_TYPE)
    string(TOUPPER ${CMAKE_BUILD_TYPE} BuildType)
endif ()

macro(display test desc summary)
  if ( ${test} )
    set(${summary} ${desc})
  else ()
    set(${summary} no)
  endif()
endmacro()

display(ENABLE_SHARED yes shared_summary)
display(ENABLE_STATIC yes static_summary)
display(BROKER_PYTHON_BINDINGS yes python_summary)
display(ZEEK_FOUND "${ZEEK_FOUND_MSG}" zeek_summary)

set(summary
    "==================|  Broker Config Summary  |===================="
    "\nVersion:         ${BROKER_VERSION}"
    "\nSO version:      ${BROKER_SOVERSION}"
    "\n"
    "\nBuild Type:      ${CMAKE_BUILD_TYPE}"
    "\nInstall prefix:  ${CMAKE_INSTALL_PREFIX}"
    "\nLibrary prefix:  ${CMAKE_INSTALL_LIBDIR}"
    "\nShared libs:     ${shared_summary}"
    "\nStatic libs:     ${static_summary}"
    "\n"
    "\nCC:              ${CMAKE_C_COMPILER}"
    "\nCFLAGS:          ${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${BuildType}}"
    "\nCXX:             ${CMAKE_CXX_COMPILER}"
    "\nCXXFLAGS:        ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${BuildType}}"
    "\n"
    "\nCAF:             ${CAF_VERSION}"
    "\nPython bindings: ${python_summary}"
    "\nZeek:            ${zeek_summary}"
    "\n=================================================================")

message("\n" ${summary} "\n")
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/config.summary ${summary})

include(UserChangedWarning)
