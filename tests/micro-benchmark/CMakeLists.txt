find_package(benchmark REQUIRED)

add_executable(micro-benchmark
  "src/main.cc"
  "src/routing-table.cc"
  "src/serialization.cc"
  "src/streaming.cc"
)

target_include_directories(micro-benchmark PRIVATE "include")

target_link_libraries(micro-benchmark PRIVATE benchmark::benchmark_main)

if (ENABLE_STATIC)
  target_link_libraries(micro-benchmark PRIVATE broker_static)
else ()
  target_link_libraries(micro-benchmark PRIVATE broker)
endif ()
