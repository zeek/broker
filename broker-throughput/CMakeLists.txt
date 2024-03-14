add_executable(broker-throughput
  broker-throughput.cc
)
target_link_libraries(broker-throughput PRIVATE ${BROKER_LIBRARY})
target_include_directories(broker-throughput PRIVATE
                           "${CMAKE_CURRENT_SOURCE_DIR}"
                           "${CMAKE_CURRENT_BINARY_DIR}")
