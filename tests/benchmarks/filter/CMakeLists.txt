if (NOT benchmark_FOUND)
  return()
endif ()

add_executable(broker-filter-benchmark
  filter.cc
)

target_include_directories(broker-filter-benchmark PRIVATE
                           "${CMAKE_CURRENT_SOURCE_DIR}")

target_link_libraries(broker-filter-benchmark
  PRIVATE
    benchmark::benchmark_main
    ${BROKER_LIBRARY})
