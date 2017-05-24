# -- Global test harness setup ------------------------------------------------

set(BROKER_TEST_TIMEOUT 20)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# -- C++ ----------------------------------------------------------------------

set(tests
  cpp/backend.cc
  cpp/core.cc
  cpp/data.cc
  cpp/event_subscriber.cc
  cpp/master.cc
  cpp/publisher.cc
  cpp/radix_tree.cc
  cpp/store.cc
  cpp/subscriber.cc
  cpp/topic.cc
  cpp/variant.cc
  test.cpp
)

# Setup correct broker library (static/shared).
if (ENABLE_SHARED)
  set(libbroker broker)
else ()
  set(libbroker broker_static)
endif ()
add_executable(broker-test ${tests})
target_link_libraries(broker-test ${libbroker})

macro(make_cpp_test suite)
  string(REPLACE " " "_" test_name ${suite})
  set(broker_test ${EXECUTABLE_OUTPUT_PATH}/broker-test)
  add_test(${test_name} ${broker_test} -v 3 -s "${suite}" ${ARGN})
  set_tests_properties(${test_name} PROPERTIES TIMEOUT ${BROKER_TEST_TIMEOUT})
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

# -- Python -------------------------------------------------------------------

if (BROKER_PYTHON_BINDINGS)
  macro(make_python_test name)
    set(script ${CMAKE_CURRENT_SOURCE_DIR}/python/${name}.py)
    set(test_name python-${name})
    add_test(NAME ${test_name}
             COMMAND ${PYTHON_EXECUTABLE} ${script}
             WORKING_DIRECTORY ${BROKER_PYTHON_STAGING_DIR})
    set_tests_properties(${test_name} PROPERTIES TIMEOUT ${BROKER_TEST_TIMEOUT})
    set_tests_properties(${test_name} PROPERTIES ENVIRONMENT
                         "PYTHONPATH=${BROKER_PYTHON_STAGING_DIR}")
  endmacro()

  make_python_test(data)
  make_python_test(communication)
endif ()
