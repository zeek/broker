add_library(micro-benchmark-base INTERFACE)

target_link_libraries(micro-benchmark-base INTERFACE benchmark::benchmark_main)

if (ENABLE_STATIC)
  target_link_libraries(micro-benchmark-base INTERFACE broker_static)
else ()
  target_link_libraries(micro-benchmark-base INTERFACE broker)
endif ()

target_include_directories(micro-benchmark-base INTERFACE "include")

add_executable(broker-data-benchmark src/data.cc)
target_link_libraries(broker-data-benchmark PRIVATE micro-benchmark-base)

# add_executable(micro-benchmark
#   "src/main.cc"
#   "src/routing-table.cc"
#   "src/serialization.cc"
#   "src/streaming.cc"
# )
#
# target_link_libraries(micro-benchmark PRIVATE benchmark::benchmark_main)
