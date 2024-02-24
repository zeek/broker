add_executable(broker-fan-out-benchmark
  fan-out.cc
)
target_link_libraries(broker-fan-out-benchmark PRIVATE ${BROKER_LIBRARY})
target_include_directories(broker-fan-out-benchmark PRIVATE
                           "${CMAKE_CURRENT_SOURCE_DIR}"
                           "${CMAKE_CURRENT_BINARY_DIR}")
