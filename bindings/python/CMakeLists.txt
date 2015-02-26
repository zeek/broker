find_package(SWIG)
find_package(PythonInterp)
find_package(PythonDev)

if ( NOT SWIG_FOUND )
    message(WARNING "Skip Python bindings: SWIG not found")
    return()
endif ()

if ( NOT PYTHONINTERP_FOUND )
    message(WARNING "Skip Python bindings: Python interpreter not found")
    return()
endif ()

if ( NOT PYTHONDEV_FOUND )
    message(WARNING "Skip Python bindings: Python include/libraries not found")
    return()
endif ()

file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/example.i "%module example %{%}")
execute_process(COMMAND ${SWIG_EXECUTABLE} -python
    ${CMAKE_CURRENT_BINARY_DIR}/example.i
    ERROR_VARIABLE SWIG_PYTHON_ERR)

if ( SWIG_PYTHON_ERR )
    message(WARNING "Skip Python bindings: SWIG test fail, ${SWIG_PYTHON_ERR}")
endif ()

include_directories(BEFORE ${PYTHON_INCLUDE_DIR})

if ( ENABLE_SHARED )
    set(libbroker broker)
else ()
    set(libbroker brokerStatic)
endif ()

include(UseSWIG)
set_property(SOURCE pybroker.i PROPERTY CPLUSPLUS ON)
swig_add_module(pybroker python pybroker.i)
swig_link_libraries(pybroker ${libbroker} ${PYTHON_LIBRARIES})

if ( BROKER_PYTHON_PREFIX )
    set(pyver ${PYTHON_VERSION_MAJOR}.${PYTHON_VERSION_MINOR})
    set(PY_MOD_INSTALL_DIR
        ${BROKER_PYTHON_PREFIX}/lib/python${pyver}/site-packages)
elseif ( BROKER_PYTHON_HOME )
    set(PY_MOD_INSTALL_DIR ${BROKER_PYTHON_HOME}/lib/python)
else ()
    execute_process(COMMAND ${PYTHON_EXECUTABLE} -c
        "from distutils.sysconfig import get_python_lib; print get_python_lib()"
                    OUTPUT_VARIABLE PYTHON_SITE_PACKAGES
                    OUTPUT_STRIP_TRAILING_WHITESPACE)
    set(PY_MOD_INSTALL_DIR ${PYTHON_SITE_PACKAGES})
endif ()

message(STATUS "Python bindings will be built and installed to: "
               ${PY_MOD_INSTALL_DIR})

install(TARGETS ${SWIG_MODULE_pybroker_REAL_NAME}
        DESTINATION ${PY_MOD_INSTALL_DIR})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/broker.py
        DESTINATION ${PY_MOD_INSTALL_DIR})
