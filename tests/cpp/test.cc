#define CAF_TEST_NO_MAIN

#include "test.hh"

#include <random>

#include <caf/test/unit_test_impl.hpp>

#include <caf/defaults.hpp>
#include <caf/io/middleman.hpp>
#include <caf/io/network/test_multiplexer.hpp>
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

template <class RandomNumberGenerator>
broker::endpoint_id random_endpoint_id(RandomNumberGenerator& rng) {
  using array_type = caf::hashed_node_id::host_id_type;
  using value_type = array_type::value_type;
  std::uniform_int_distribution<> d{0, std::numeric_limits<value_type>::max()};
  array_type result;
  for (auto& x : result)
    x = static_cast<value_type>(d(rng));
  return caf::make_node_id(d(rng), result);
}

}//

base_fixture::base_fixture()
  : ep(make_config()),
    sys(ep.system()),
    self(sys),
    sched(dynamic_cast<scheduler_type&>(sys.scheduler())) {
  init_socket_api();
  // This somewhat convoluted way to fill the ids map makes sure that we fill
  // up the map in a way that the values are sorted by their ID.
  std::minstd_rand rng{0xDEADC0DE};
  std::vector<endpoint_id> id_list;
  for (auto i = 0; i < 'Z' - 'A'; ++i)
    id_list.emplace_back(random_endpoint_id(rng));
  std::sort(id_list.begin(), id_list.end());
  char id = 'A';
  for (auto& val : id_list)
    ids[id++] = val;
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
  cfg.load<io::middleman, io::network::test_multiplexer>();
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
