if (NOT benchmark_FOUND)
  return()
endif ()

add_executable(broker-routing-table-benchmark
  routing-table.cc
)

target_include_directories(broker-routing-table-benchmark PRIVATE
                           "${CMAKE_CURRENT_SOURCE_DIR}")

target_link_libraries(broker-routing-table-benchmark
  PRIVATE
    benchmark::benchmark_main
    ${BROKER_LIBRARY})
