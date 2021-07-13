#define CAF_TEST_NO_MAIN

#include "test.hh"

#include <random>

#include <caf/test/unit_test_impl.hpp>

#include <caf/defaults.hpp>
#include <caf/test/dsl.hpp>

#include "broker/config.hh"
#include "broker/core_actor.hh"

#ifdef BROKER_WINDOWS
#undef ERROR // The Windows headers fail if this macro is predefined.
#include "Winsock2.h"
#endif

using namespace caf;
using namespace broker;

namespace {

std::string_view uuid_strings[] = {
  "685a1674-e15c-11eb-ba80-0242ac130004",
  "685a1a2a-e15c-11eb-ba80-0242ac130004",
  "685a1b2e-e15c-11eb-ba80-0242ac130004",
  "685a1bec-e15c-11eb-ba80-0242ac130004",
  "685a1caa-e15c-11eb-ba80-0242ac130004",
  "685a1d5e-e15c-11eb-ba80-0242ac130004",
  "685a1e1c-e15c-11eb-ba80-0242ac130004",
  "685a1ed0-e15c-11eb-ba80-0242ac130004",
  "685a20d8-e15c-11eb-ba80-0242ac130004",
  "685a21a0-e15c-11eb-ba80-0242ac130004",
  "685a2254-e15c-11eb-ba80-0242ac130004",
  "685a2308-e15c-11eb-ba80-0242ac130004",
  "685a23bc-e15c-11eb-ba80-0242ac130004",
  "685a2470-e15c-11eb-ba80-0242ac130004",
  "685a2524-e15c-11eb-ba80-0242ac130004",
  "685a27ae-e15c-11eb-ba80-0242ac130004",
  "685a286c-e15c-11eb-ba80-0242ac130004",
  "685a2920-e15c-11eb-ba80-0242ac130004",
  "685a29d4-e15c-11eb-ba80-0242ac130004",
  "685a2a88-e15c-11eb-ba80-0242ac130004",
  "685a2b3c-e15c-11eb-ba80-0242ac130004",
  "685a2bf0-e15c-11eb-ba80-0242ac130004",
  "685a2e2a-e15c-11eb-ba80-0242ac130004",
  "685a2ef2-e15c-11eb-ba80-0242ac130004",
  "685a2fa6-e15c-11eb-ba80-0242ac130004",
  "685a305a-e15c-11eb-ba80-0242ac130004",
};

} // namespace

base_fixture::base_fixture()
  : ep(make_config()),
    sys(ep.system()),
    self(sys),
    sched(dynamic_cast<scheduler_type&>(sys.scheduler())) {
  init_socket_api();
  char id = 'A';
  while (id <= 'Z')
    ids[id++] = *caf::make_uuid(uuid_strings[id - 'A']);
}

base_fixture::~base_fixture() {
  run();
  // Our core might do some messaging in its dtor, hence we need to make sure
  // messages are handled when enqueued to avoid blocking.
  sched.inline_all_enqueues();
  deinit_socket_api();
}

void base_fixture::init_socket_api() {
#ifdef BROKER_WINDOWS
  WSADATA WinsockData;
  if (WSAStartup(MAKEWORD(2, 2), &WinsockData) != 0) {
    fprintf(stderr, "WSAStartup failed\n");
    abort();
  }
#endif
}

void base_fixture::deinit_socket_api() {
#ifdef BROKER_WINDOWS
  WSACleanup();
#endif
}

char base_fixture::id_by_value(const broker::endpoint_id& value) {
  for (const auto& [key, val] : ids)
    if (val == value)
      return key;
  FAIL("value not found: " << value);
}

configuration base_fixture::make_config() {
  broker_options options;
  options.disable_ssl = true;
  configuration cfg{options};
  test_coordinator_fixture<configuration>::init_config(cfg);
  return cfg;
}

void base_fixture::run() {
  while (sched.has_job() || sched.has_pending_timeout()) {
    sched.run();
    sched.trigger_timeouts();
  }
}

void base_fixture::consume_message() {
  if (!sched.try_run_once())
    CAF_FAIL("no message to consume");
}

int main(int argc, char** argv) {
  caf::init_global_meta_objects<caf::id_block::broker_test>();
  broker::configuration::init_global_state();
  //if (! broker::logger::file(broker::logger::debug, "broker-unit-test.log"))
  //  return 1;
  return test::main(argc, argv);
}
