if ( ENABLE_SHARED )
    set(libbroker broker)
else ()
    set(libbroker brokerStatic)
endif ()

add_executable(scratch scratch.c)
target_link_libraries(scratch ${libbroker})
