# -- Project Setup ------------------------------------------------------------

cmake_minimum_required(VERSION 3.15 FATAL_ERROR)
project(broker C CXX)

include(CMakePackageConfigHelpers)
include(GNUInstallDirs)
include(cmake/CommonCMakeConfig.cmake)
include(cmake/ZeekBundle.cmake)

set(BROKER_TEST_TIMEOUT 60)

set(BROKER_CLANG_TIDY "clang-tidy" CACHE STRING "Path to clang-tidy executable")

get_directory_property(parent_dir PARENT_DIRECTORY)
if (parent_dir)
    set(DISABLE_BROKER_EXTRA_TOOLS ON)
    set(broker_is_subproject ON)
else ()
    set(broker_is_subproject OFF)
    if (NOT DEFINED DISABLE_BROKER_EXTRA_TOOLS)
        set(DISABLE_BROKER_EXTRA_TOOLS OFF)
    endif ()
endif ()
unset(parent_dir)

if (MSVC)
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

    if (ENABLE_CCACHE)
        find_program(CCACHE_PROGRAM ccache)

        if (NOT CCACHE_PROGRAM)
            message(FATAL_ERROR "ccache not found")
        endif ()

        message(STATUS "Using ccache: ${CCACHE_PROGRAM}")
        set(CMAKE_C_COMPILER_LAUNCHER ${CCACHE_PROGRAM})
        set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE_PROGRAM})
    endif ()

    if (BROKER_SANITIZERS)
        set(_sanitizer_flags "-fsanitize=${BROKER_SANITIZERS}")
        set(_sanitizer_flags "${_sanitizer_flags} -fno-omit-frame-pointer")
        set(_sanitizer_flags "${_sanitizer_flags} -fno-optimize-sibling-calls")

        # Technically, then we also need to use the compiler to drive linking and
        # give the sanitizer flags there, too.  However, CMake, by default, uses
        # the compiler for linking and so the flags automatically get used.  See
        # https://cmake.org/pipermail/cmake/2014-August/058268.html
        set(CAF_EXTRA_FLAGS "${_sanitizer_flags}")

        # Set EXTRA_FLAGS if broker isn't being built as part of a Zeek build.
        # The Zeek build sets it otherwise.
        if (NOT ZEEK_SANITIZERS)
            set(EXTRA_FLAGS "${EXTRA_FLAGS} ${_sanitizer_flags}")
        endif ()
    endif ()

endif ()

if (MSVC)
    # Allow more sections in object files, otherwise Broker fails to compile.
    set(EXTRA_FLAGS "${EXTRA_FLAGS} /bigobj")
    # Similar to -funsigned-char on other platforms.
    set(EXTRA_FLAGS "${EXTRA_FLAGS} /J")
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

include(cmake/RequireCXXStd.cmake)
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
check_cxx_source_compiles(
    "
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

    set(atomic_64bit_ops_test
        "
    #include <atomic>
    struct s64 { char a, b, c, d, e, f, g, h; };
    int main() { std::atomic<s64> x; x.store({}); x.load(); return 0; }
  ")
    check_cxx_source_compiles("${atomic_64bit_ops_test}" atomic64_builtin)

    if (NOT atomic64_builtin)
        set(CMAKE_REQUIRED_LIBRARIES atomic)
        check_cxx_source_compiles("${atomic_64bit_ops_test}" atomic64_with_lib)
        set(CMAKE_REQUIRED_LIBRARIES)

        if (atomic64_with_lib)
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
elseif (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    set(BROKER_LINUX true)
elseif (${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
    set(BROKER_FREEBSD true)
elseif (WIN32)
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
endif ()
set(LINK_LIBS ${LINK_LIBS} OpenSSL::SSL OpenSSL::Crypto)

function (add_bundled_caf)
    # Disable unnecessary features and make sure CAF builds static libraries.
    set(CAF_ENABLE_EXAMPLES OFF)
    set(CAF_ENABLE_TESTING OFF)
    set(CAF_ENABLE_TOOLS OFF)
    set(BUILD_SHARED_LIBS OFF)
    if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24")
        add_subdirectory(caf EXCLUDE_FROM_ALL SYSTEM)
    else ()
        add_subdirectory(caf EXCLUDE_FROM_ALL)
    endif ()
    # Disable a few false positives / irrelevant warnings while building CAF.
    if (CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID MATCHES "GNU")
        target_compile_options(caf_internal INTERFACE -Wno-invalid-offsetof -Wno-documentation)
    endif ()
endfunction ()

# Bundle prometheus-cpp. This approach is currently experimental.
if (CMAKE_MSVC_RUNTIME_LIBRARY)
    set(msvc_zeekbundle_args CMAKE_MSVC_RUNTIME_LIBRARY=${CMAKE_MSVC_RUNTIME_LIBRARY}
                             CMAKE_MSVC_RUNTIME_LIBRARY_FLAG=${CMAKE_MSVC_RUNTIME_LIBRARY_FLAG})
endif ()
zeekbundle_add(
    NAME
    prometheus-cpp
    FETCH
    SOURCE_DIR
    ${CMAKE_CURRENT_SOURCE_DIR}/auxil/prometheus-cpp
    CONFIGURE
    ${msvc_zeekbundle_args}
    ENABLE_COMPRESSION=OFF
    ENABLE_PUSH=OFF)
unset(msvc_zeekbundle_args)

# Bundle all prometheus-cpp libraries that we need as single interface library.
add_library(broker-prometheus-cpp INTERFACE)
target_link_libraries(broker-prometheus-cpp INTERFACE $<BUILD_INTERFACE:prometheus-cpp::core>
                                                      $<BUILD_INTERFACE:prometheus-cpp::pull>)
# Prometheus-cpp sets C++ 11 via CMake property, but we need C++ 17.
target_compile_features(broker-prometheus-cpp INTERFACE cxx_std_17)
# Must be exported to keep CMake happy, even though we only need it internally.
install(TARGETS broker-prometheus-cpp EXPORT BrokerTargets DESTINATION ${CMAKE_INSTALL_LIBDIR})

# NOTE: building and linking against an external CAF version is NOT supported!
#       This variable is FOR DEVELOPMENT ONLY. The only officially supported CAF
#       version is the bundled version!
if (CAF_ROOT)
    message(STATUS "Using system CAF version ${CAF_VERSION}")
    find_package(CAF REQUIRED COMPONENTS test io core net)
    list(APPEND LINK_LIBS CAF::core CAF::io CAF::net)
    set(BROKER_USE_EXTERNAL_CAF ON)
else ()
    # Note: we use the object libraries here to avoid having dependencies on the
    # actual CAF targets that would force consumers of Broker to have CMake being
    # able to find a CAF installation.
    message(STATUS "Using bundled CAF")
    add_bundled_caf()
    set(BROKER_USE_EXTERNAL_CAF OFF)
endif ()

if (NOT BROKER_DISABLE_TESTS)
    enable_testing()
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
install(CODE "MESSAGE(STATUS \"Removing: ${CMAKE_INSTALL_PREFIX}/include/broker\")"
        CODE "file(REMOVE_RECURSE \"${CMAKE_INSTALL_PREFIX}/include/broker\")")

# Install all headers except the files from broker/internal.
install(
    DIRECTORY libbroker/broker
    DESTINATION include
    FILES_MATCHING
    PATTERN "*.hh"
    PATTERN "libbroker/broker/internal" EXCLUDE
    PATTERN "*.test.hh" EXCLUDE)

if (NOT BROKER_EXTERNAL_SQLITE_TARGET)
    include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR}/3rdparty)
    set_source_files_properties(3rdparty/sqlite3.c PROPERTIES COMPILE_FLAGS
                                                              -DSQLITE_OMIT_LOAD_EXTENSION)
    list(APPEND OPTIONAL_SRC ${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/sqlite3.c)
else ()
    list(APPEND LINK_LIBS ${BROKER_EXTERNAL_SQLITE_TARGET})
endif ()

add_subdirectory(libbroker)

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
    set_target_properties(
        ${BROKER_LIBRARY} PROPERTIES CXX_CLANG_TIDY
                                     "${BROKER_CLANG_TIDY};--config-file=${tidyCfgFile}")
    target_compile_definitions(${BROKER_LIBRARY} PRIVATE ${BROKER_CLANG_TIDY_DEF})
    configure_file(.clang-tidy .clang-tidy COPYONLY)
endif ()

# -- Tools and benchmarks -----------------------------------------------------

if (NOT DISABLE_BROKER_EXTRA_TOOLS)
    add_subdirectory(broker-node)
    add_subdirectory(broker-pipe)
    add_subdirectory(broker-throughput)
endif ()

# -- Bindings -----------------------------------------------------------------

if (NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/bindings/python/3rdparty/pybind11/CMakeLists.txt")
    message(WARNING "Skipping Python bindings: pybind11 submodule not available")
    set(DISABLE_PYTHON_BINDINGS true)
endif ()

if (NOT DISABLE_PYTHON_BINDINGS)
    set(Python_ADDITIONAL_VERSIONS 3)
    find_package(Python 3.9 COMPONENTS Interpreter Development)

    if (NOT Python_FOUND)
        message(STATUS "Skipping Python bindings: Python interpreter not found")
    else ()
        set(BROKER_PYTHON_BINDINGS true)
        set(BROKER_PYTHON_STAGING_DIR ${CMAKE_CURRENT_BINARY_DIR}/python)
        add_subdirectory(bindings/python)
    endif ()
endif ()

# -- Zeek ---------------------------------------------------------------------

if (NOT "${ZEEK_EXECUTABLE}" STREQUAL "")
    set(ZEEK_FOUND true)
    set(ZEEK_FOUND_MSG "${ZEEK_EXECUTABLE}")
else ()
    set(ZEEK_FOUND false)
    find_file(ZEEK_PATH_DEV zeek-path-dev.sh PATHS ${CMAKE_CURRENT_BINARY_DIR}/../../../build
              NO_DEFAULT_PATH)
    if (EXISTS ${ZEEK_PATH_DEV})
        set(ZEEK_FOUND true)
        set(ZEEK_FOUND_MSG "${ZEEK_PATH_DEV}")
    endif ()
endif ()

# -- Unit Tests ---------------------------------------------------------------

if (NOT BROKER_DISABLE_TESTS)
    add_subdirectory(tests)
endif ()

# -- Documentation ------------------------------------------------------------

if (NOT WIN32 AND NOT BROKER_DISABLE_DOCS)
    add_subdirectory(doc)
endif ()

# -- CMake package install ----------------------------------------------------

export(EXPORT BrokerTargets FILE BrokerTargets.cmake)

install(EXPORT BrokerTargets DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/Broker")

configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/libbroker/BrokerConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/BrokerConfig.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/Broker")

write_basic_package_version_file("${CMAKE_CURRENT_BINARY_DIR}/BrokerConfigVersion.cmake"
                                 VERSION ${BROKER_VERSION} COMPATIBILITY ExactVersion)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/BrokerConfig.cmake"
              "${CMAKE_CURRENT_BINARY_DIR}/BrokerConfigVersion.cmake"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/Broker")

# -- Build Summary ------------------------------------------------------------

if (CMAKE_BUILD_TYPE)
    string(TOUPPER ${CMAKE_BUILD_TYPE} BuildType)
endif ()

macro (display test desc summary)
    if (${test})
        set(${summary} ${desc})
    else ()
        set(${summary} no)
    endif ()
endmacro ()

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
