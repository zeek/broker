add_executable(broker-pipe broker-pipe.cc)
target_link_libraries(broker-pipe PRIVATE ${BROKER_LIBRARY} CAF::core)
target_include_directories(broker-pipe PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}"
                                               "${CMAKE_CURRENT_BINARY_DIR}")
