if ( ENABLE_SHARED )
    set(libbroker broker)
else ()
    set(libbroker brokerStatic)
endif ()

add_executable(scratch scratch.c)
target_link_libraries(scratch ${libbroker})

add_executable(scratch++ scratch.cc)
target_link_libraries(scratch++ ${libbroker})

add_executable(endpoint_test endpoint_test.cc)
target_link_libraries(endpoint_test ${libbroker})
