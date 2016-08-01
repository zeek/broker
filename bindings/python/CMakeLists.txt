# Setup build instructions for Python module.
add_library(pybroker MODULE _broker.cpp)

# Set includes.
target_include_directories(pybroker
  PUBLIC ${PYBIND11_INCLUDE_DIRS} ${PYTHON_INCLUDE_DIRS})

# Set libraries to link against.
target_compile_options(pybroker PRIVATE "-fvisibility=hidden")
if (ENABLE_SHARED)
  set(libbroker broker)
else ()
  set(libbroker brokerStatic)
endif ()
target_link_libraries(pybroker PUBLIC ${libbroker} ${PYTHON_LIBRARIES})
if (ROCKSDB_FOUND)
  target_link_libraries(pybroker PUBLIC ${ROCKSDB_LIBRARIES})
endif ()
if (APPLE)
  # Support multiple Python installations.
  target_link_libraries(pybroker PRIVATE "-undefined dynamic_lookup")
endif ()

set_target_properties(pybroker PROPERTIES OUTPUT_NAME "_broker")
# By setting an empty prefix, we can invoke the Python executable in the same
# path as the module. Then 'import _broker' Just Works.
set_target_properties(pybroker PROPERTIES PREFIX "")

# Check for Link Time Optimization support (GCC/Clang)
include(CheckCXXCompilerFlag)
if (NOT CMAKE_BUILD_TYPE MATCHES DEBUG)
  check_cxx_compiler_flag("-flto" HAS_LTO_FLAG)
  if (HAS_LTO_FLAG)
    target_compile_options(pybroker PRIVATE -flto)
  endif()
endif ()

# Strip unnecessary sections of the binary on Linux/Mac OS
if (CMAKE_STRIP)
  if(APPLE)
    add_custom_command(TARGET pybroker POST_BUILD
                       COMMAND ${CMAKE_STRIP} -u -r $<TARGET_FILE:pybroker>)
  else()
    add_custom_command(TARGET pybroker POST_BUILD
                       COMMAND ${CMAKE_STRIP} $<TARGET_FILE:pybroker>)
  endif()
endif ()

# Figure out Python module install directory.
if (BROKER_PYTHON_PREFIX)
  set(pyver ${PYTHON_VERSION_MAJOR}.${PYTHON_VERSION_MINOR})
  set(PY_MOD_INSTALL_DIR
      ${BROKER_PYTHON_PREFIX}/lib/python${pyver}/site-packages)
elseif (BROKER_PYTHON_HOME)
  set(PY_MOD_INSTALL_DIR ${BROKER_PYTHON_HOME}/lib/python)
else ()
  execute_process(COMMAND ${PYTHON_EXECUTABLE} -c
    "from distutils.sysconfig import get_python_lib; print get_python_lib()"
    OUTPUT_VARIABLE python_site_packages
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  set(PY_MOD_INSTALL_DIR ${python_site_packages})
endif ()
message(STATUS "Python bindings will be built and installed to:")
message(STATUS "  ${PY_MOD_INSTALL_DIR}")

install(TARGETS pybroker DESTINATION ${PY_MOD_INSTALL_DIR})
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/broker.py
        DESTINATION ${PY_MOD_INSTALL_DIR})
