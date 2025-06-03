# -- system tests via btest --------------------------------------------------

add_subdirectory(btest)

add_subdirectory(benchmarks)

# -- Python -------------------------------------------------------------------

if (BROKER_PYTHON_BINDINGS)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/python/run-zeek.in
                   ${CMAKE_CURRENT_BINARY_DIR}/python/run-zeek)

    macro (make_python_test name)
        set(script ${CMAKE_CURRENT_SOURCE_DIR}/python/${name}.py)
        set(test_name python-${name})
        add_test(NAME ${test_name} COMMAND ${Python_EXECUTABLE} ${script} ${ARGN}
                 WORKING_DIRECTORY ${BROKER_PYTHON_STAGING_DIR})
        set_tests_properties(${test_name} PROPERTIES TIMEOUT ${BROKER_TEST_TIMEOUT})
        set_tests_properties(
            ${test_name}
            PROPERTIES ENVIRONMENT
                       "PYTHONPATH=${BROKER_PYTHON_STAGING_DIR};BROKER_TEST_DIR=${BROKER_TEST_DIR}")
    endmacro ()

    if (ZEEK_FOUND)
        make_python_test(zeek)
        make_python_test(zeek-unsafe-types)
    endif ()

    make_python_test(communication)
    make_python_test(data)
    make_python_test(forwarding)
    make_python_test(ssl-tests)
    make_python_test(store)
    make_python_test(topic)
    make_python_test(zeek-module)
    # TODO: re-enable after updating generator files or adding backwards compatiblity
    # make_python_test(broker-cluster-benchmark
    #                  $<TARGET_FILE:broker-cluster-benchmark>)
    #
    # # allow some extra time for the benchmark integration test
    # set_tests_properties(python-broker-cluster-benchmark PROPERTIES TIMEOUT 120)
endif ()
