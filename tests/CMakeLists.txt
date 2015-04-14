if ( ENABLE_SHARED )
    set(libbroker broker)
else ()
    set(libbroker brokerStatic)
endif ()

set(BROKER_TEST_TIMEOUT 10)

macro(unit_test name)
    if ( BROKER_TEST_C_API )
        add_executable(${name} ${name}.c)
    else ()
        add_executable(${name} ${name}.cc)
    endif ()

    target_link_libraries(${name} ${libbroker})

    if ( ${ARGC} EQUAL 1 )
        add_test(NAME ${name} COMMAND ${name})
        set_tests_properties(${name} PROPERTIES TIMEOUT ${BROKER_TEST_TIMEOUT})
    else ()
        foreach ( _arg ${ARGN} )
            add_test(NAME ${name}_${_arg} COMMAND ${name} ${_arg})
            set_tests_properties(${name}_${_arg} PROPERTIES TIMEOUT
                                 ${BROKER_TEST_TIMEOUT})
        endforeach ()
    endif ()
endmacro()

macro(unit_test_python script)
    if ( BROKER_PYTHON_BINDINGS )
        add_test(NAME ${script} COMMAND ${PYTHON_EXECUTABLE}
                                        ${CMAKE_CURRENT_SOURCE_DIR}/${script})
        set_tests_properties(${name} PROPERTIES TIMEOUT ${BROKER_TEST_TIMEOUT})
        set_tests_properties(${script} PROPERTIES ENVIRONMENT
                 "PYTHONPATH=${CMAKE_CURRENT_BINARY_DIR}/../bindings/python")
    endif ()
endmacro()

set(backends memory;sqlite)

if ( ROCKSDB_FOUND )
    add_definitions(-DHAVE_ROCKSDB)
    set(backends ${backends} rocksdb)
endif ()

unit_test(test_print)
unit_test(test_print_remote)
unit_test(test_event local;remote)
unit_test(test_log   local;remote)
unit_test(test_store_master)
unit_test(test_store_clone)
unit_test(test_store_frontend)
unit_test(test_store_remote)
unit_test(test_store_backend ${backends})
unit_test(test_store_expiry ${backends})
unit_test(test_subscription_matching)
unit_test(test_variant)
unit_test(test_optional)
unit_test(test_data)
unit_test(test_network_types)
unit_test(test_radix_tree)

unit_test_python(test_messages.py)
unit_test_python(test_store.py)

set(BROKER_TEST_C_API true)

unit_test(test_data_c)
unit_test(test_print_c)
unit_test(test_print_remote_c)
unit_test(test_network_types_c)
unit_test(test_store_c)
