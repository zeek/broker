# -- Global test harness setup ------------------------------------------------

include(CTest)
enable_testing()

set(BROKER_TEST_TIMEOUT 10)

# FIXME
#set(backends memory;sqlite)
#if (ROCKSDB_FOUND)
#  add_definitions(-DHAVE_ROCKSDB)
#  set(backends ${backends} rocksdb)
#endif ()

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# -- C++ ----------------------------------------------------------------------

set(tests
  test.cpp
  cpp/endpoint.cc
  cpp/radix_tree.cc
)
#unit_test(test_print)
#unit_test(test_print_remote)
#unit_test(test_event local;remote)
#unit_test(test_log   local;remote)
#unit_test(test_store_master)
#unit_test(test_store_clone)
#unit_test(test_store_frontend)
#unit_test(test_store_remote)
#unit_test(test_store_backend ${backends})
#unit_test(test_store_expiry ${backends})
#unit_test(test_subscription_matching)
#unit_test(test_variant)
#unit_test(test_data)
#unit_test(test_network_types)

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
      string(REGEX REPLACE "#define SUITE \(.*\)" "\\1" suite ${line})
      list(APPEND suites ${suite})
    endif()
  endforeach()
endforeach()
list(REMOVE_DUPLICATES suites)

foreach(suite ${suites})
  make_cpp_test("${suite}")
endforeach ()

# -- Python -------------------------------------------------------------------

macro(make_python_test script)
  if (BROKER_PYTHON_BINDINGS)
    add_test(NAME ${script} COMMAND ${PYTHON_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/${script})
    set_tests_properties(${name} PROPERTIES TIMEOUT ${BROKER_TEST_TIMEOUT})
    set_tests_properties(${script} PROPERTIES ENVIRONMENT
      "PYTHONPATH=${CMAKE_CURRENT_BINARY_DIR}/../bindings/python")
  endif ()
endmacro()

# FIXME
#make_python_test(test_messages.py)
#make_python_test(test_store.py)

# -- C ------------------------------------------------------------------------

# FIXME
#make_c_test(test_data_c)
#make_c_test(test_print_c)
#make_c_test(test_print_remote_c)
#make_c_test(test_network_types_c)
#make_c_test(test_store_c)
