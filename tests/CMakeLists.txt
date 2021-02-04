# -- Global test harness setup ------------------------------------------------

set(BROKER_TEST_TIMEOUT 60)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/cpp)

# -- C++ ----------------------------------------------------------------------

set(tests
  cpp/backend.cc
  cpp/core.cc
  cpp/data.cc
  cpp/detail/central_dispatcher.cc
  cpp/detail/data_generator.cc
  cpp/detail/generator_file_writer.cc
  cpp/detail/meta_command_writer.cc
  cpp/detail/meta_data_writer.cc
  cpp/error.cc
  cpp/filter_type.cc
  cpp/integration.cc
  cpp/master.cc
  cpp/publisher.cc
  cpp/publisher_id.cc
  cpp/radix_tree.cc
  cpp/ssl.cc
  cpp/status.cc
  cpp/status_subscriber.cc
  cpp/store.cc
  cpp/store_event.cc
  cpp/subscriber.cc
  cpp/test.cc
  cpp/topic.cc
  cpp/zeek.cc
)

# Setup correct broker library (static/shared).
if (ENABLE_SHARED)
  set(libbroker broker)
else ()
  set(libbroker broker_static)
endif ()
add_executable(broker-test ${tests})
target_link_libraries(broker-test ${libbroker} CAF::test)

set(BROKER_TEST_DIR "${CMAKE_CURRENT_SOURCE_DIR}")

macro(make_cpp_test suite)
  string(REPLACE " " "_" test_name ${suite})
  set(test_verbosity 4)
  if (${test_name} STREQUAL radix_tree)
    set(test_verbosity 3)  # otherwise it just produces way too much output
  endif ()
  add_test(NAME ${test_name} COMMAND broker-test -v ${test_verbosity} -s "${suite}" ${ARGN})
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

## -- Benchmark ---------------------------------------------------------------

add_executable(broker-benchmark benchmark/broker-benchmark.cc)
target_link_libraries(broker-benchmark ${libbroker})
install(TARGETS broker-benchmark DESTINATION bin)

add_executable(broker-cluster-benchmark benchmark/broker-cluster-benchmark.cc)
target_link_libraries(broker-cluster-benchmark ${libbroker})
install(TARGETS broker-cluster-benchmark DESTINATION bin)

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
  endif ()

  make_python_test(communication)
  make_python_test(data)
  make_python_test(forwarding)
  make_python_test(ssl-tests)
  make_python_test(store)
  make_python_test(topic)
  make_python_test(broker-cluster-benchmark
                   $<TARGET_FILE:broker-cluster-benchmark>)

  # allow some extra time for the benchmark integration test
  set_tests_properties(python-broker-cluster-benchmark PROPERTIES TIMEOUT 120)
endif ()
