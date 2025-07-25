# The internal module, which is wrapped by Python code.
add_library(_broker MODULE _broker.cpp data.cpp enums.cpp store.cpp zeek.cpp)

# Stage the Python wrapper along with the internal module in the public
# "broker" module.
set(BROKER_PYTHON_MODULE_DIR ${BROKER_PYTHON_STAGING_DIR}/broker)
set_target_properties(
    _broker
    PROPERTIES OUTPUT_NAME "_broker"
               LIBRARY_OUTPUT_DIRECTORY ${BROKER_PYTHON_MODULE_DIR}
               # By setting an empty prefix, we can invoke the Python
               # executable in the same path as the module. Then
               # 'import _broker' Just Works.
               PREFIX "")

# Stage Python scripts.
add_custom_target(
    python-scripts-stage
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/broker
            ${BROKER_PYTHON_MODULE_DIR}
    COMMENT "Staging Python scripts in ${BROKER_PYTHON_MODULE_DIR}"
    VERBATIM)

# Whenever we build the bindings, we also ensure that we stage the current
# scripts along with it.
add_dependencies(_broker python-scripts-stage)

# Set includes.
target_include_directories(_broker PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/pybind11/include/
                                          ${Python_INCLUDE_DIRS})

# Set libraries to link against.
if (NOT WIN32)
    target_compile_options(_broker PRIVATE "-fvisibility=hidden")
endif ()

if (ENABLE_SHARED)
    set(libbroker broker)
else ()
    set(libbroker broker_static)
endif ()
target_link_libraries(_broker PUBLIC ${libbroker} ${Python_LIBRARIES})
if (APPLE)
    # Support multiple Python installations.
    target_link_libraries(_broker PRIVATE "-undefined dynamic_lookup")
endif ()

# Strip unnecessary sections of the binary on Linux/Mac OS.
if (CMAKE_STRIP)
    if (APPLE)
        add_custom_command(TARGET _broker POST_BUILD COMMAND ${CMAKE_STRIP} -u -r
                                                             $<TARGET_FILE:_broker>)
    else ()
        add_custom_command(TARGET _broker POST_BUILD COMMAND ${CMAKE_STRIP} $<TARGET_FILE:_broker>)
    endif ()
endif ()

if (NOT PY_MOD_INSTALL_DIR)
    # Figure out Python module install directory.
    if (BROKER_PYTHON_PREFIX)
        set(pyver ${Python_VERSION_MAJOR}.${Python_VERSION_MINOR})
        file(TO_CMAKE_PATH "${BROKER_PYTHON_PREFIX}/lib/python${pyver}/site-packages"
             PY_MOD_INSTALL_DIR)
    elseif (BROKER_PYTHON_HOME)
        file(TO_CMAKE_PATH "${BROKER_PYTHON_HOME}/lib/python" PY_MOD_INSTALL_DIR)
    else ()
        set(PY_MOD_INSTALL_DIR "${Python_SITEARCH}")
    endif ()
endif ()
message(STATUS "Python bindings will be built and installed to:")
message(STATUS "  ${PY_MOD_INSTALL_DIR}")

install(TARGETS _broker DESTINATION ${PY_MOD_INSTALL_DIR}/broker)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/broker DESTINATION ${PY_MOD_INSTALL_DIR}
        REGEX "/\\..*" EXCLUDE)
