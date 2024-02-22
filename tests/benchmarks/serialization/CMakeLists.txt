if (NOT benchmark_FOUND)
  return()
endif ()

add_executable(broker-serialization-benchmark
  serialization.cc
)

target_include_directories(broker-serialization-benchmark PRIVATE
                           "${CMAKE_CURRENT_SOURCE_DIR}")

target_link_libraries(broker-serialization-benchmark
  PRIVATE
    benchmark::benchmark_main
    ${BROKER_LIBRARY})
