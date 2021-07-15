#define CAF_TEST_NO_MAIN

#include "test.hh"

#include <random>

#include <caf/test/unit_test_impl.hpp>

#include <caf/defaults.hpp>
#include <caf/test/dsl.hpp>

#include "broker/config.hh"
#include "broker/core_actor.hh"
#include "broker/detail/flow_controller_callback.hh"

#ifdef BROKER_WINDOWS
#undef ERROR // The Windows headers fail if this macro is predefined.
#include "Winsock2.h"
#endif

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
  cfg.set("broker.disable-connector", true);
  return cfg;
}

namespace {

struct bridge_state {
  static inline const char* name = "broker.test.bridge";
};

using bridge_actor = caf::stateful_actor<bridge_state>;

} // namespace

caf::actor base_fixture::bridge(const endpoint_state& left,
                                const endpoint_state& right) {
  using actor_t = bridge_actor;
  using node_message_publisher = caf::async::publisher<node_message>;
  using proc = caf::flow::broadcaster_impl<node_message>;
  using proc_ptr = caf::intrusive_ptr<proc>;
  proc_ptr left_to_right;
  proc_ptr right_to_left;
  auto& sys = left.hdl.home_system();
  caf::event_based_actor* self = nullptr;
  std::function<void()> launch;
  std::tie(self, launch) = sys.make_flow_coordinator<actor_t>();
  left_to_right.emplace(self);
  right_to_left.emplace(self);
  left_to_right
    ->as_observable() //
    .for_each([](const node_message& msg) { BROKER_DEBUG("->" << msg); });
  right_to_left //
    ->as_observable()
    .for_each([](const node_message& msg) { BROKER_DEBUG("<-" << msg); });
  auto connect_left = [=](node_message_publisher left_input) {
    self->observe(left_input).attach(left_to_right->as_observer());
    return self->to_async_publisher(right_to_left->as_observable());
  };
  auto connect_right = [=](node_message_publisher right_input) {
    self->observe(right_input).attach(right_to_left->as_observer());
    return self->to_async_publisher(left_to_right->as_observable());
  };
  using detail::flow_controller_callback;
  auto lcb
    = detail::make_flow_controller_callback([=](detail::flow_controller* ptr) {
        auto dptr = dynamic_cast<alm::stream_transport*>(ptr);
        auto fn = [=](node_message_publisher in) { return connect_left(in); };
        auto err = dptr->init_new_peer(right.id, right.ts, right.filter, fn);
      });
  caf::anon_send(left.hdl, std::move(lcb));
  auto rcb
    = detail::make_flow_controller_callback([=](detail::flow_controller* ptr) {
        auto dptr = dynamic_cast<alm::stream_transport*>(ptr);
        auto fn = [=](node_message_publisher in) { return connect_right(in); };
        auto err = dptr->init_new_peer(left.id, left.ts, left.filter, fn);
      });
  caf::anon_send(right.hdl, std::move(rcb));
  auto hdl = caf::actor{self};
  launch();
  return hdl;
}

caf::actor base_fixture::bridge(const endpoint& left, const endpoint& right) {
  auto& left_state = deref<core_actor_type>(left.core()).state;
  auto& right_state = deref<core_actor_type>(right.core()).state;
  return bridge(endpoint_state{left.node_id(), left_state.timestamp(),
                               left_state.filter()->read(), left.core()},
                endpoint_state{right.node_id(), right_state.timestamp(),
                               right_state.filter()->read(), right.core()});
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
  return caf::test::main(argc, argv);
}
