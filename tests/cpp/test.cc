#define CAF_TEST_NO_MAIN

#include "test.hh"

#include <caf/test/unit_test_impl.hpp>

#include <caf/defaults.hpp>
#include <caf/io/middleman.hpp>
#include <caf/io/network/test_multiplexer.hpp>
#include <caf/test/dsl.hpp>

#include "broker/config.hh"
#include "broker/internal/configuration_access.hh"
#include "broker/internal/core_actor.hh"
#include "broker/internal/endpoint_access.hh"
#include "broker/internal/endpoint_id.hh"

#ifdef BROKER_WINDOWS
#include "Winsock2.h"
#endif

using namespace caf;
using namespace broker;

namespace {

std::string_view id_strings[] = {
  "585ED2E2AFAFD9CABE2B3E785C6BD13FFC7DAA4A#72",
  "36CE117717441F2D33A7D577859DD8D62A3B5C33#80",
  "A19A1322788D83FB023A1A0C622100F72CE993CC#7",
  "1AF111469B4410776C93F4F4C3470E3E12DC4270#13",
  "8520FD7D03848D7007E3FFEBB670787F02EEFB45#39",
  "2E851102B9DC65F590536754C54302C056840F46#44",
  "D77D95067726851EFB12A921BC326D89FB8B6C13#27",
  "F3B4667DFB3D20D4A57EB9122D7340BC58EFFA06#76",
  "5486B594548D434FA960D944670BDC231DBA5D2A#79",
  "B49D88EAF2037CF8CF1BFB5346DCF675B3C79FBD#98",
  "AA8E46AFC177702E81087F1401CBBFF9E43E6DC0#85",
  "B38B7D480A59253E181C97F0F3C9FFE65F1377AD#77",
  "998673238266C7BF8DC46195548F3DF03A94537F#3",
  "376498E443FF78A8E9505E272CD94734629B988D#83",
  "12B26ABFF91DC1384FAA2F813CE1FFFDED9486B1#56",
  "5BAF51CFEA36E049B5411A81A4C574929197DCA8#8",
  "E528954FF843E2812DAD92B438B8354507DCC729#37",
  "55C047C8B73F4A2AB626D9770C1C2D5CECF5A73A#44",
  "397E2A5BA2AB590253F5D9C420B32F7C1432797E#68",
  "EC9596FEAF0B241F6E9885165C8AC21BB5066098#48",
  "891BE1CA09BC40905E6905247DA48D6A313A9A91#66",
  "66A5751A79731556A72CD4BE5B1CBEF8CF7BE4A7#1",
  "EE2026EF36D9FF991B2D9322C3F9C3B169891FF0#18",
  "4564568D77936944F263884C4539C44E4EFC8DD8#8",
  "5A3244BDA8FB805AC07E8477CE51000D31BE461A#6",
  "A4DF42092AFF6AF904607357A2B73FF873F1A877#40",
};

} // namespace

base_fixture::base_fixture()
  : ep(make_config()),
    sys(internal::endpoint_access{&ep}.sys()),
    self(sys),
    sched(dynamic_cast<scheduler_type&>(sys.scheduler())) {
  init_socket_api();
  for (char id = 'A'; id <= 'Z'; ++id) {
    auto index = id - 'A';
    str_ids[id] = id_strings[index];
    ids[id] = internal::endpoint_id_from_string(id_strings[index]);
  }
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

configuration base_fixture::make_config() {
  broker_options options;
  options.disable_ssl = true;
  configuration cfg{options};
  auto& nat_cfg = internal::configuration_access{&cfg}.cfg();
  test_coordinator_fixture<caf::actor_system_config>::init_config(nat_cfg);
  nat_cfg.load<io::middleman, io::network::test_multiplexer>();
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
  broker::configuration::init_global_state();
  //if (! broker::logger::file(broker::logger::debug, "broker-unit-test.log"))
  //  return 1;
  return test::main(argc, argv);
}
