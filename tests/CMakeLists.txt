# -- Global test harness setup ------------------------------------------------

set(BROKER_TEST_TIMEOUT 60)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/cpp)

# -- C++ ----------------------------------------------------------------------

set(tests
  cpp/alm/multipath.cc
  cpp/alm/routing_table.cc
  cpp/backend.cc
  cpp/data.cc
  cpp/detail/peer_status_map.cc
  cpp/domain_options.cc
  cpp/error.cc
  cpp/filter_type.cc
  # cpp/integration.cc
  cpp/internal/channel.cc
  cpp/internal/core_actor.cc
  cpp/internal/json_type_mapper.cc
  # cpp/internal/data_generator.cc
  # cpp/internal/generator_file_writer.cc
  # cpp/internal/meta_command_writer.cc
  # cpp/internal/meta_data_writer.cc
  cpp/internal/metric_collector.cc
  cpp/internal/metric_exporter.cc
  cpp/master.cc
  cpp/publisher.cc
  cpp/radix_tree.cc
  # cpp/ssl.cc
  cpp/status.cc
  cpp/status_subscriber.cc
  cpp/store.cc
  cpp/store_event.cc
  cpp/subscriber.cc
  cpp/system/peering.cc
  cpp/system/shutdown.cc
  cpp/telemetry/histogram.cc
  cpp/test.cc
  cpp/topic.cc
  cpp/zeek.cc
)

# Our sytem testing suites require `socketpair`, but Windows lacks this API.
if (NOT MSVC)
  list(APPEND tests
    cpp/system/shutdown.cc
  )
endif()

add_executable(broker-test ${tests})
target_link_libraries(broker-test PRIVATE ${BROKER_LIBRARY} CAF::test CAF::core CAF::net)

set(BROKER_TEST_DIR "${CMAKE_CURRENT_SOURCE_DIR}")

macro(make_cpp_test suite)
  string(REPLACE " " "_" test_name ${suite})
  set(test_verbosity 4)
  if (${test_name} STREQUAL radix_tree)
    set(test_verbosity 3)  # otherwise it just produces way too much output
  endif ()
  add_test(NAME ${test_name} COMMAND broker-test -v ${test_verbosity} -s "^${suite}$" ${ARGN})
  set_tests_properties(${test_name} PROPERTIES TIMEOUT ${BROKER_TEST_TIMEOUT})
  set_tests_properties(${test_name} PROPERTIES ENVIRONMENT
                       "BROKER_TEST_DIR=${BROKER_TEST_DIR}")
endmacro()

# Find all test suites.
foreach(test ${tests})
  file(STRINGS ${test} contents)
  foreach(line ${contents})
    if ("${line}" MATCHES "SUITE")
      string(REGEX REPLACE ".* SUITE (.*)" "\\1" suite ${line})
      list(APPEND suites ${suite})
    endif()
  endforeach()
endforeach()
list(REMOVE_DUPLICATES suites)

foreach(suite ${suites})
  make_cpp_test("${suite}")
endforeach ()

# -- system tests via btest --------------------------------------------------

add_subdirectory(btest)

# -- Benchmark ----------------------------------------------------------------

add_executable(broker-benchmark benchmark/broker-benchmark.cc)
target_link_libraries(broker-benchmark ${BROKER_LIBRARY})
install(TARGETS broker-benchmark DESTINATION bin)

add_executable(broker-fan-out benchmark/broker-fan-out.cc)
target_link_libraries(broker-fan-out ${BROKER_LIBRARY})

# add_executable(broker-cluster-benchmark benchmark/broker-cluster-benchmark.cc)
# target_link_libraries(broker-cluster-benchmark ${libbroker} CAF::core CAF::openssl CAF::io)
# install(TARGETS broker-cluster-benchmark DESTINATION bin)

# -- Python -------------------------------------------------------------------

if (BROKER_PYTHON_BINDINGS)
  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/python/run-zeek.in
                 ${CMAKE_CURRENT_BINARY_DIR}/python/run-zeek)

  macro(make_python_test name)
    set(script ${CMAKE_CURRENT_SOURCE_DIR}/python/${name}.py)
    set(test_name python-${name})
    add_test(NAME ${test_name}
             COMMAND ${PYTHON_EXECUTABLE} ${script} ${ARGN}
             WORKING_DIRECTORY ${BROKER_PYTHON_STAGING_DIR})
    set_tests_properties(${test_name} PROPERTIES TIMEOUT ${BROKER_TEST_TIMEOUT})
    set_tests_properties(${test_name} PROPERTIES ENVIRONMENT
                         "PYTHONPATH=${BROKER_PYTHON_STAGING_DIR};BROKER_TEST_DIR=${BROKER_TEST_DIR}")
  endmacro()

  if (ZEEK_FOUND)
    make_python_test(zeek)
    make_python_test(zeek-unsafe-types)
  endif ()

  make_python_test(communication)
  make_python_test(data)
  make_python_test(forwarding)
  # make_python_test(ssl-tests): re-enable after implementing SSL support
  make_python_test(store)
  make_python_test(topic)
  make_python_test(zeek-module)
  # TODO: re-enable after updating generator files or adding backwards compatiblity
  # make_python_test(broker-cluster-benchmark
  #                  $<TARGET_FILE:broker-cluster-benchmark>)
  #
  # # allow some extra time for the benchmark integration test
  # set_tests_properties(python-broker-cluster-benchmark PROPERTIES TIMEOUT 120)
endif ()

# -- Python -------------------------------------------------------------------

if (BROKER_ENABLE_MICRO_BENCHMARKS)
  add_subdirectory(micro-benchmark)
endif ()
