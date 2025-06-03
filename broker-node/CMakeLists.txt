add_executable(broker-node broker-node.cc)
target_link_libraries(broker-node PRIVATE ${BROKER_LIBRARY} CAF::core)
target_include_directories(broker-node PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}"
                                               "${CMAKE_CURRENT_BINARY_DIR}")
