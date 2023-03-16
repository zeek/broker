configure_file(${CMAKE_CURRENT_SOURCE_DIR}/broker/config.hh.in
               ${CMAKE_CURRENT_BINARY_DIR}/broker/config.hh)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/broker/config.hh
        DESTINATION include/broker)

set(BROKER_SRC
  # src/detail/core_recorder.cc
  # src/detail/generator_file_reader.cc
  # src/detail/generator_file_writer.cc
  # src/gateway.cc
  # src/internal/data_generator.cc
  # src/internal/generator_file_reader.cc
  # src/internal/generator_file_writer.cc
  # src/internal/meta_command_writer.cc
  # src/internal/meta_data_writer.cc
  ${OPTIONAL_SRC}
  broker/address.cc
  broker/alm/multipath.cc
  broker/alm/routing_table.cc
  broker/builder.cc
  broker/command_envelope.cc
  broker/configuration.cc
  broker/convert.cc
  broker/data.cc
  broker/data_envelope.cc
  broker/detail/abstract_backend.cc
  broker/detail/filesystem.cc
  broker/detail/flare.cc
  broker/detail/make_backend.cc
  broker/detail/memory_backend.cc
  broker/detail/monotonic_buffer_resource.cc
  broker/detail/opaque_type.cc
  broker/detail/peer_status_map.cc
  broker/detail/prefix_matcher.cc
  broker/detail/sink_driver.cc
  broker/detail/source_driver.cc
  broker/detail/sqlite_backend.cc
  broker/detail/store_state.cc
  broker/domain_options.cc
  broker/endpoint.cc
  broker/endpoint_id.cc
  broker/endpoint_info.cc
  broker/entity_id.cc
  broker/envelope.cc
  broker/error.cc
  broker/event_observer.cc
  broker/filter_type.cc
  broker/format/bin.cc
  broker/format/json.cc
  broker/hub.cc
  broker/internal/clone_actor.cc
  broker/internal/connector.cc
  broker/internal/connector_adapter.cc
  broker/internal/core_actor.cc
  broker/internal/flare_actor.cc
  broker/internal/hub_impl.cc
  broker/internal/json.cc
  broker/internal/json_client.cc
  broker/internal/json_type_mapper.cc
  broker/internal/master_actor.cc
  broker/internal/metric_factory.cc
  broker/internal/peering.cc
  broker/internal/pending_connection.cc
  broker/internal/println.cc
  broker/internal/publisher_queue.cc
  broker/internal/radix_filter.cc
  broker/internal/store_actor.cc
  broker/internal/subscriber_queue.cc
  broker/internal/web_socket.cc
  broker/internal/wire_format.cc
  broker/internal_command.cc
  broker/logger.cc
  broker/mailbox.cc
  broker/network_info.cc
  broker/overflow_policy.cc
  broker/p2p_message_type.cc
  broker/peer_status.cc
  broker/ping_envelope.cc
  broker/pong_envelope.cc
  broker/port.cc
  broker/publisher.cc
  broker/routing_update_envelope.cc
  broker/shutdown_options.cc
  broker/status.cc
  broker/status_subscriber.cc
  broker/store.cc
  broker/store_event.cc
  broker/subnet.cc
  broker/subscriber.cc
  broker/time.cc
  broker/topic.cc
  broker/variant.cc
  broker/variant_data.cc
  broker/variant_list.cc
  broker/variant_set.cc
  broker/variant_table.cc
  broker/variant_tag.cc
  broker/version.cc
  broker/worker.cc
  broker/zeek.cc
)

if (ENABLE_SHARED)
  add_library(broker SHARED ${BROKER_SRC})
  set_target_properties(broker PROPERTIES
                        SOVERSION ${BROKER_SOVERSION}
                        VERSION ${BROKER_VERSION_MAJOR}.${BROKER_VERSION_MINOR}
                        MACOSX_RPATH true
                        OUTPUT_NAME broker)
  target_link_libraries(broker PUBLIC ${LINK_LIBS})
  target_link_libraries(
    broker
    PRIVATE
      CAF::core
      CAF::io
      CAF::net
      broker-prometheus-cpp)
  install(TARGETS broker
          EXPORT BrokerTargets
          DESTINATION ${CMAKE_INSTALL_LIBDIR})
  target_include_directories(broker PUBLIC
                             $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
                             $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
                             $<INSTALL_INTERFACE:include>)
endif ()

if (ENABLE_STATIC)
  add_library(broker_static STATIC ${BROKER_SRC})
  set_target_properties(broker_static PROPERTIES OUTPUT_NAME broker)
  if (NOT DISABLE_PYTHON_BINDINGS)
    set_target_properties(broker_static PROPERTIES POSITION_INDEPENDENT_CODE ON)
  endif()
  target_link_libraries(broker_static PUBLIC ${LINK_LIBS})
  target_link_libraries(
    broker_static
    PRIVATE
      CAF::core
      CAF::io
      CAF::net
      broker-prometheus-cpp)
  install(TARGETS broker_static
          EXPORT BrokerTargets
          DESTINATION ${CMAKE_INSTALL_LIBDIR})
  target_include_directories(broker_static PUBLIC
                             $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
                             $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
                             $<INSTALL_INTERFACE:include>)
endif ()

if (ENABLE_SHARED)
  set(main_lib_target broker)
else()
  set(main_lib_target broker_static)
endif()

set(BROKER_LIBRARY ${main_lib_target} PARENT_SCOPE)

if ( BROKER_DISABLE_TESTS )
  return()
endif ()

set(BROKER_TEST_SRC
  # broker/internal/data_generator.test.cc
  # broker/internal/generator_file_writer.test.cc
  # broker/internal/json_type_mapper.test.cc
  # broker/internal/meta_command_writer.test.cc
  # broker/internal/meta_data_writer.test.cc
  broker/alm/multipath.test.cc
  broker/alm/routing_table.test.cc
  broker/backend.test.cc
  broker/broker-test.test.cc
  broker/builder.test.cc
  broker/data.test.cc
  broker/detail/peer_status_map.test.cc
  broker/domain_options.test.cc
  broker/envelope.test.cc
  broker/error.test.cc
  broker/filter_type.test.cc
  broker/format/bin.test.cc
  broker/format/json.test.cc
  broker/hub.test.cc
  broker/internal/channel.test.cc
  broker/internal/json.test.cc
  broker/internal/radix_filter.test.cc
  broker/internal/wire_format.test.cc
  broker/peering.test.cc
  broker/radix_tree.test.cc
  broker/shutdown.test.cc
  broker/status.test.cc
  broker/store.test.cc
  broker/store_event.test.cc
  broker/topic.test.cc
  broker/variant.test.cc
  broker/zeek.test.cc
)

# Our sytem testing suites require `socketpair`, but Windows lacks this API.
# if (NOT MSVC)
#   list(APPEND tests
#     cpp/system/shutdown.cc
#   )
# endif()

add_executable(broker-test ${BROKER_TEST_SRC})
target_link_libraries(
  broker-test
  PRIVATE
    ${main_lib_target}
    CAF::core
    CAF::io
    CAF::net
    CAF::test
    broker-prometheus-cpp)

foreach(file_path ${BROKER_TEST_SRC})
  get_filename_component(test_dir ${file_path} DIRECTORY)
  get_filename_component(test_file ${file_path} NAME_WE)
  string(REPLACE "/" "." test_name ${test_dir}/${test_file})
  set_source_files_properties(${file_path} PROPERTIES COMPILE_DEFINITIONS SUITE=${test_name})
  set(test_verbosity 4)
  if (${test_name} STREQUAL broker.radix_tree)
    set(test_verbosity 3)  # otherwise it just produces way too much output
  endif ()
  add_test(NAME ${test_name} COMMAND broker-test -v ${test_verbosity} -s "^${test_name}$" ${ARGN})
  set_tests_properties(${test_name} PROPERTIES TIMEOUT ${BROKER_TEST_TIMEOUT})
  set_tests_properties(${test_name} PROPERTIES ENVIRONMENT
                       "BROKER_TEST_DIR=${BROKER_TEST_DIR}")
endforeach ()
