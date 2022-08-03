#define SUITE core

#include "broker/internal/core_actor.hh"

#include "test.hh"

#include <caf/attach_stream_sink.hpp>
#include <caf/attach_stream_source.hpp>
#include <caf/test/io_dsl.hpp>

#include "broker/configuration.hh"
#include "broker/endpoint.hh"
#include "broker/internal/logger.hh"

using broker::internal::core_actor_type;
using broker::internal::facade;
using broker::internal::native;

using namespace broker;

namespace atom = broker::internal::atom;

namespace {

struct driver_state {
  using buf_type = std::vector<data_message>;
  bool restartable = false;
  buf_type xs;
  static inline const char* name = "driver";
  void reset() {
    xs = data_msgs({{"a", 0},
                    {"b", true},
                    {"a", 1},
                    {"a", 2},
                    {"b", false},
                    {"b", true},
                    {"a", 3},
                    {"b", false},
                    {"a", 4},
                    {"a", 5}});
  }
  driver_state() {
    reset();
  }
};

caf::behavior driver(caf::stateful_actor<driver_state>* self,
                     const caf::actor& sink, bool restartable) {
  self->state.restartable = restartable;
  auto ptr =
    attach_stream_source(
      self,
      // Destination.
      sink,
      // Initialize send buffer with 10 elements.
      [](caf::unit_t&) {
        // nop
      },
      // Get next element.
      [=](caf::unit_t&, caf::downstream<data_message>& out, size_t num) {
        auto& xs = self->state.xs;
        auto n = std::min(num, xs.size());
        if (n == 0)
          return;
        for (size_t i = 0u; i < n; ++i)
          out.push(xs[i]);
        xs.erase(xs.begin(), xs.begin() + static_cast<ptrdiff_t>(n));
      },
      // Did we reach the end?.
      [=](const caf::unit_t&) {
        auto& st = self->state;
        return !st.restartable && st.xs.empty();
      })
      .ptr();
  return {
    [=](atom::restart) {
      self->state.reset();
      self->state.restartable = false;
      ptr->push();
    },
  };
}

struct consumer_state {
  std::vector<data_message> xs;
  static const char* name;
};

const char* consumer_state::name = "consumer";

caf::behavior consumer(caf::stateful_actor<consumer_state>* self,
                       filter_type ts, const caf::actor& src) {
  self->send(self * src, atom::join_v, std::move(ts));
  return {
    [=](caf::stream<data_message> in) {
      attach_stream_sink(
        self,
        // Input stream.
        in,
        // Initialize state.
        [](caf::unit_t&) {
          // nop
        },
        // Process single element.
        [=](caf::unit_t&, data_message x) {
          self->state.xs.emplace_back(std::move(x));
        },
        // Cleanup.
        [](caf::unit_t&) {
          // nop
        });
    },
    [=](atom::get) { return self->state.xs; },
  };
}

} // namespace

// Simulates a simple setup with two cores, where data flows from core1 to
// core2.
CAF_TEST(local_peers) {
  // Spawn core actors and disable events.
  broker_options options;
  options.disable_ssl = true;
  auto core1 = sys.spawn<core_actor_type>(filter_type{"a", "b", "c"}, options);
  auto core2 = sys.spawn<core_actor_type>(filter_type{"a", "b", "c"}, options);
  anon_send(core1, atom::no_events_v);
  anon_send(core2, atom::no_events_v);
  run();
  CAF_MESSAGE("connect a consumer (leaf) to core2");
  auto leaf = sys.spawn(consumer, filter_type{"b"}, core2);
  CAF_MESSAGE("core1: " << to_string(core1));
  CAF_MESSAGE("core2: " << to_string(core2));
  CAF_MESSAGE("leaf: " << to_string(leaf));
  consume_message();
  expect((atom::join, filter_type),
         from(leaf).to(core2).with(_, filter_type{"b"}));
  run();
  // Initiate handshake between core1 and core2.
  self->send(core1, atom::peer_v, core2);
  expect((atom::peer, caf::actor), from(self).to(core1).with(_, core2));
  // Check if core1 reports a pending peer.
  CAF_MESSAGE("query peer information from core1");
  sched.inline_next_enqueue();
  self->request(core1, infinite, atom::get_v, atom::peer_v)
    .receive(
      [&](const std::vector<peer_info>& xs) {
        CAF_REQUIRE_EQUAL(xs.size(), 1u);
        CAF_REQUIRE_EQUAL(xs.front().status, peer_status::connecting);
      },
      [&](const caf::error& err) { CAF_FAIL(err); });
  CAF_MESSAGE("run handshake between peers");
  run();
  // Check if core1 & core2 both report each other as peered.
  CAF_MESSAGE("query peer information from core1");
  sched.inline_next_enqueue();
  self->request(core1, infinite, atom::get_v, atom::peer_v)
    .receive(
      [&](const std::vector<peer_info>& xs) {
        CAF_REQUIRE_EQUAL(xs.size(), 1u);
        CAF_REQUIRE_EQUAL(xs.front().status, peer_status::peered);
      },
      [&](const caf::error& err) { CAF_FAIL(err); });
  CAF_MESSAGE("query peer information from core2");
  sched.inline_next_enqueue();
  self->request(core2, infinite, atom::get_v, atom::peer_v)
    .receive(
      [&](const std::vector<peer_info>& xs) {
        CAF_REQUIRE_EQUAL(xs.size(), 1u);
        CAF_REQUIRE_EQUAL(xs.front().status, peer_status::peered);
      },
      [&](const caf::error& err) { CAF_FAIL(err); });
  CAF_MESSAGE("spin up driver on core1");
  auto d1 = sys.spawn(driver, core1, false);
  CAF_MESSAGE("driver: " << to_string(d1));
  run();
  CAF_MESSAGE("check log of the consumer after the driver is done");
  using buf = std::vector<data_message>;
  self->send(leaf, atom::get_v);
  sched.prioritize(leaf);
  consume_message();
  self->receive([](const buf& xs) {
    auto expected =
      data_msgs({{"b", true}, {"b", false}, {"b", true}, {"b", false}});
    CAF_REQUIRE_EQUAL(xs, expected);
  });
  CAF_MESSAGE("send message 'directly' from core1 to core2 (bypass streaming)");
  anon_send(core1, atom::publish_v,
            endpoint_info{facade(core2.node()), std::nullopt},
            make_data_message(topic("b"), data{true}));
  expect((atom::publish, endpoint_info, data_message),
         from(_).to(core1).with(_, _, _));
  expect((atom::publish, atom::local, data_message),
         from(core1).to(core2).with(_, _,
                                    make_data_message(topic("b"), data{true})));
  run();
  CAF_MESSAGE("check log of the consumer again");
  self->send(leaf, atom::get_v);
  sched.prioritize(leaf);
  consume_message();
  self->receive([](const buf& xs) {
    auto expected = data_msgs(
      {{"b", true}, {"b", false}, {"b", true}, {"b", false}, {"b", true}});
    CAF_REQUIRE_EQUAL(xs, expected);
  });
  CAF_MESSAGE("unpeer core1 from core2");
  anon_send(core1, atom::unpeer_v, core2);
  run();
  CAF_MESSAGE("check whether both core1 and core2 report no more peers");
  sched.inline_next_enqueue();
  self->request(core1, infinite, atom::get_v, atom::peer_v)
    .receive(
      [&](const std::vector<peer_info>& xs) {
        CAF_REQUIRE_EQUAL(xs.size(), 0u);
      },
      [&](const caf::error& err) { CAF_FAIL(err); });
  sched.inline_next_enqueue();
  self->request(core2, infinite, atom::get_v, atom::peer_v)
    .receive(
      [&](const std::vector<peer_info>& xs) {
        CAF_REQUIRE_EQUAL(xs.size(), 0u);
      },
      [&](const caf::error& err) { CAF_FAIL(err); });
  CAF_MESSAGE("shutdown core actors");
  anon_send_exit(core1, caf::exit_reason::user_shutdown);
  anon_send_exit(core2, caf::exit_reason::user_shutdown);
  anon_send_exit(leaf, caf::exit_reason::user_shutdown);
}

// Simulates a simple triangle setup where core1 peers with core2, and core2
// peers with core3. Data flows from core1 to core2 and core3.
CAF_TEST(triangle_peering) {
  // Spawn core actors and disable events.
  broker_options options;
  options.disable_ssl = true;
  auto core1 = sys.spawn<core_actor_type>(filter_type{"a", "b", "c"}, options);
  auto core2 = sys.spawn<core_actor_type>(filter_type{"a", "b", "c"}, options);
  auto core3 = sys.spawn<core_actor_type>(filter_type{"a", "b", "c"}, options);
  anon_send(core1, atom::no_events_v);
  anon_send(core2, atom::no_events_v);
  anon_send(core3, atom::no_events_v);
  run();
  // Connect a consumer (leaf) to core1 (this consumer never receives data,
  // because data isn't forwarded to local subscribers).
  auto leaf1 = sys.spawn(consumer, filter_type{"b"}, core1);
  // Connect a consumer (leaf) to core2.
  auto leaf2 = sys.spawn(consumer, filter_type{"b"}, core2);
  run();
  // Connect a consumer (leaf) to core3.
  auto leaf3 = sys.spawn(consumer, filter_type{"b"}, core3);
  run();
  // Initiate handshake between core1 and core2.
  self->send(core1, atom::peer_v, core2);
  expect((atom::peer, caf::actor), from(self).to(core1).with(_, core2));
  // Check if core1 reports a pending peer.
  CAF_MESSAGE("query peer information from core1");
  sched.inline_next_enqueue();
  self->request(core1, infinite, atom::get_v, atom::peer_v)
    .receive(
      [&](const std::vector<peer_info>& xs) {
        CAF_REQUIRE_EQUAL(xs.size(), 1u);
        CAF_REQUIRE_EQUAL(xs.front().status, peer_status::connecting);
      },
      [&](const caf::error& err) { CAF_FAIL(err); });
  // Step #1: core1  --->    ('peer', filter_type)    ---> core2
  expect((atom::peer, filter_type, caf::actor),
         from(core1).to(core2).with(_, filter_type{"a", "b", "c"}, core1));
  run();
  // Initiate handshake between core2 and core3.
  self->send(core2, atom::peer_v, core3);
  expect((atom::peer, caf::actor), from(self).to(core2).with(_, core3));
  // Check if core2 reports a pending peer.
  CAF_MESSAGE("query peer information from core2");
  sched.inline_next_enqueue();
  self->request(core2, infinite, atom::get_v, atom::peer_v)
    .receive(
      [&](const std::vector<peer_info>& xs) {
        CAF_REQUIRE_EQUAL(xs.size(), 2u);
        CAF_REQUIRE_EQUAL(xs.front().status, peer_status::peered);
        CAF_REQUIRE_EQUAL(xs.back().status, peer_status::connecting);
      },
      [&](const caf::error& err) { CAF_FAIL(err); });
  // Perform further handshake steps.
  run();
  // Check if all cores properly report peering setup.
  CAF_MESSAGE("query peer information from core1");
  sched.inline_next_enqueue();
  self->request(core1, infinite, atom::get_v, atom::peer_v)
    .receive(
      [&](const std::vector<peer_info>& xs) {
        CAF_REQUIRE_EQUAL(xs.size(), 1u);
        CAF_REQUIRE_EQUAL(xs.front().status, peer_status::peered);
      },
      [&](const caf::error& err) { CAF_FAIL(err); });
  CAF_MESSAGE("query peer information from core2");
  sched.inline_next_enqueue();
  self->request(core2, infinite, atom::get_v, atom::peer_v)
    .receive(
      [&](const std::vector<peer_info>& xs) {
        CAF_REQUIRE_EQUAL(xs.size(), 2u);
        CAF_REQUIRE_EQUAL(xs.front().status, peer_status::peered);
        CAF_REQUIRE_EQUAL(xs.back().status, peer_status::peered);
      },
      [&](const caf::error& err) { CAF_FAIL(err); });
  CAF_MESSAGE("query peer information from core3");
  sched.inline_next_enqueue();
  self->request(core3, infinite, atom::get_v, atom::peer_v)
    .receive(
      [&](const std::vector<peer_info>& xs) {
        CAF_REQUIRE_EQUAL(xs.size(), 1u);
        CAF_REQUIRE_EQUAL(xs.front().status, peer_status::peered);
      },
      [&](const caf::error& err) { CAF_FAIL(err); });
  // Spin up driver on core1.
  auto d1 = sys.spawn(driver, core1, false);
  CAF_MESSAGE("d1: " << to_string(d1));
  run();
  // Check log of the consumers.
  using buf = std::vector<data_message>;
  auto expected =
    data_msgs({{"b", true}, {"b", false}, {"b", true}, {"b", false}});
  for (auto& leaf : {leaf2, leaf3}) {
    self->send(leaf, atom::get_v);
    sched.prioritize(leaf);
    consume_message();
    self->receive([&](const buf& xs) { CAF_REQUIRE_EQUAL(xs, expected); });
  }
  // Make sure leaf1 never received any data.
  self->send(leaf1, atom::get_v);
  sched.prioritize(leaf1);
  consume_message();
  self->receive([&](const buf& xs) { CAF_CHECK_EQUAL(xs, buf{}); });
  // Shutdown.
  CAF_MESSAGE("Shutdown core actors.");
  anon_send_exit(core1, caf::exit_reason::user_shutdown);
  anon_send_exit(core2, caf::exit_reason::user_shutdown);
  anon_send_exit(core3, caf::exit_reason::user_shutdown);
}

// Simulates a simple setup where core1 peers with core2 and starts sending
// data. After receiving a couple of messages, core2 terminates and core3
// starts peering. Core3 must receive all remaining messages.
// peers with core3. Data flows from core1 to core2 and core3.
CAF_TEST(sequenced_peering) {
  // Spawn core actors and disable events.
  broker_options options;
  options.disable_ssl = true;
  auto core1 = sys.spawn<core_actor_type>(filter_type{"a", "b", "c"}, options);
  auto core2 = sys.spawn<core_actor_type>(filter_type{"a", "b", "c"}, options);
  auto core3 = sys.spawn<core_actor_type>(filter_type{"a", "b", "c"}, options);
  CAF_MESSAGE(BROKER_ARG(core1));
  CAF_MESSAGE(BROKER_ARG(core2));
  CAF_MESSAGE(BROKER_ARG(core3));
  anon_send(core1, atom::no_events_v);
  anon_send(core2, atom::no_events_v);
  anon_send(core3, atom::no_events_v);
  run();
  // Connect a consumer (leaf) to core2.
  auto leaf1 = sys.spawn(consumer, filter_type{"b"}, core2);
  CAF_MESSAGE(BROKER_ARG(leaf1));
  run();
  // Connect a consumer (leaf) to core3.
  auto leaf2 = sys.spawn(consumer, filter_type{"b"}, core3);
  CAF_MESSAGE(BROKER_ARG(leaf2));
  run();
  // Initiate handshake between core1 and core2.
  self->send(core1, atom::peer_v, core2);
  expect((atom::peer, caf::actor), from(self).to(core1).with(_, core2));
  // Check if core1 reports a pending peer.
  CAF_MESSAGE("query peer information from core1");
  sched.inline_next_enqueue();
  self->request(core1, infinite, atom::get_v, atom::peer_v)
    .receive(
      [&](const std::vector<peer_info>& xs) {
        CAF_REQUIRE_EQUAL(xs.size(), 1u);
        CAF_REQUIRE_EQUAL(xs.front().status, peer_status::connecting);
      },
      [&](const caf::error& err) { CAF_FAIL(err); });
  expect((atom::peer, filter_type, caf::actor),
         from(core1).to(core2).with(_, filter_type{"a", "b", "c"}, core1));
  run();
  CAF_MESSAGE("spin up driver and transmit first half of the data");
  auto d1 = sys.spawn(driver, core1, true);
  CAF_MESSAGE(BROKER_ARG(d1));
  run();
  // Check log of the consumer on core2.
  using buf = std::vector<data_message>;
  auto expected =
    data_msgs({{"b", true}, {"b", false}, {"b", true}, {"b", false}});
  self->send(leaf1, atom::get_v);
  sched.prioritize(leaf1);
  consume_message();
  self->receive([&](const buf& xs) { CAF_REQUIRE_EQUAL(xs, expected); });
  CAF_MESSAGE("kill core2");
  anon_send_exit(core2, caf::exit_reason::user_shutdown);
  run();
  CAF_MESSAGE("make sure core1 sees no peer anymore");
  sched.inline_next_enqueue();
  self->request(core1, infinite, atom::get_v, atom::peer_v)
    .receive(
      [&](const std::vector<peer_info>& xs) {
        CAF_REQUIRE_EQUAL(xs.size(), 0u);
      },
      [&](const caf::error& err) { CAF_FAIL(err); });
  // Initiate handshake between core1 and core3.
  self->send(core1, atom::peer_v, core3);
  expect((atom::peer, caf::actor), from(self).to(core1).with(_, core3));
  // Check if core1 reports a pending peer.
  CAF_MESSAGE("query peer information from core1");
  sched.inline_next_enqueue();
  self->request(core1, infinite, atom::get_v, atom::peer_v)
    .receive(
      [&](const std::vector<peer_info>& xs) {
        CAF_REQUIRE_EQUAL(xs.size(), 1u);
        CAF_REQUIRE_EQUAL(xs.front().status, peer_status::connecting);
      },
      [&](const caf::error& err) { CAF_FAIL(err); });
  // Step #1: core1  --->    ('peer', filter_type)    ---> core3
  expect((atom::peer, filter_type, caf::actor),
         from(core1).to(core3).with(_, filter_type{"a", "b", "c"}, core1));
  run();
  CAF_MESSAGE("restart driver and send second half of the data");
  anon_send(d1, atom::restart_v);
  run();
  // Check log of the consumer on core3.
  sched.inline_next_enqueue();
  self->request(leaf2, infinite, atom::get_v)
    .receive([&](const buf& xs) { CAF_CHECK_EQUAL(xs, expected); },
             [&](const caf::error& err) { CAF_FAIL(err); });
  // Shutdown.
  CAF_MESSAGE("Shutdown core actors.");
  anon_send_exit(core1, caf::exit_reason::user_shutdown);
  anon_send_exit(core3, caf::exit_reason::user_shutdown);
}

CAF_TEST_FIXTURE_SCOPE_END()

namespace {

struct error_signaling_fixture : base_fixture {
  caf::actor core1;
  caf::actor core2;
  status_subscriber es;

  error_signaling_fixture() : es(ep.make_status_subscriber(true)) {
    es.set_rate_calculation(false);
    core1 = native(ep.core());
    CAF_MESSAGE(BROKER_ARG(core1));
    anon_send(core1, atom::subscribe_v, filter_type{"a", "b", "c"});
    core2 = sys.spawn<core_actor_type>(filter_type{"a", "b", "c"},
                                       ep.options());
    CAF_MESSAGE(BROKER_ARG(core2));
    run();
    CAF_MESSAGE("init done");
  }
};

struct event_visitor {
  using result_type = std::variant<caf::none_t, sc, ec>;
  using vector_type = std::vector<result_type>;

  result_type operator()(const broker::error& x) {
    return {static_cast<ec>(x.code())};
  }

  result_type operator()(const broker::status& x) {
    return {x.code()};
  }

  template <class T>
  result_type operator()(const T&) {
    return {caf::none};
  }

  template <class T>
  static vector_type convert(const std::vector<T>& xs) {
    event_visitor f;
    std::vector<result_type> ys;
    for (auto& x : xs)
      ys.emplace_back(visit(f, x));
    return ys;
  }
};

#define BROKER_CHECK_LOG(InputLog, ...)                                        \
  {                                                                            \
    auto log = event_visitor::convert(InputLog);                               \
    event_visitor::vector_type expected_log{__VA_ARGS__};                      \
    CAF_CHECK_EQUAL(log, expected_log);                                        \
  }                                                                            \
  CAF_VOID_STMT

} // namespace

CAF_TEST_FIXTURE_SCOPE(error_signaling, error_signaling_fixture)

// Simulates a connection abort after sending 'peer' message ("stage #0").
CAF_TEST(failed_handshake_stage0) {
  // Spawn core actors and disable events.
  // Initiate handshake between core1 and core2, but kill core2 right away.
  self->send(core1, atom::peer_v, core2);
  anon_send_exit(core2, caf::exit_reason::kill);
  expect((atom::peer, caf::actor), from(self).to(core1).with(_, core2));
  run();
  BROKER_CHECK_LOG(es.poll(), ec::peer_unavailable);
}

// Simulates a connection abort after receiving stage #1 handshake.
CAF_TEST(failed_handshake_stage1) {
  // Initiate handshake between core1 and core2, but kill core2 right away.
  self->send(core2, atom::peer_v, core1);
  expect((atom::peer, caf::actor), from(self).to(core2).with(_, core1));
  expect((atom::peer, filter_type, caf::actor),
         from(core2).to(core1).with(_, filter_type{"a", "b", "c"}, core2));
  anon_send_exit(core2, caf::exit_reason::kill);
  run();
  BROKER_CHECK_LOG(es.poll(), sc::peer_added, sc::peer_lost);
}

// Simulates a connection abort after sending 'peer' message ("stage #0").
CAF_TEST(failed_handshake_stage2) {
  CAF_MESSAGE("initiate handshake between core1 and core2");
  self->send(core1, atom::peer_v, core2);
  expect((atom::peer, caf::actor), from(self).to(core1).with(_, core2));
  expect((atom::peer, filter_type, caf::actor),
         from(_).to(core2).with(_, filter_type{"a", "b", "c"}, core1));
  CAF_MESSAGE("send kill to core2");
  anon_send_exit(core2, caf::exit_reason::kill);
  CAF_MESSAGE("have core1 handle the pending handshake");
  expect((caf::open_stream_msg), from(_).to(core1).with(_, _, _, _, _, false));
  CAF_MESSAGE("run remaining messages");
  run();
  CAF_MESSAGE("check log of the event subscriber");
  BROKER_CHECK_LOG(es.poll(), ec::peer_unavailable);
}

// Simulates a connection abort after receiving stage #1 handshake.
CAF_TEST(failed_handshake_stage3) {
  // Initiate handshake between core1 and core2, but kill core2 right away.
  self->send(core2, atom::peer_v, core1);
  expect((atom::peer, caf::actor), from(self).to(core2).with(_, core1));
  expect((atom::peer, filter_type, caf::actor),
         from(core2).to(core1).with(_, filter_type{"a", "b", "c"}, core2));
  expect((caf::open_stream_msg), from(_).to(core2).with(_, core1, _, _, false));
  anon_send_exit(core2, caf::exit_reason::kill);
  expect((caf::open_stream_msg), from(_).to(core1).with(_, core2, _, _, false));
  run();
  BROKER_CHECK_LOG(es.poll(), sc::peer_added, sc::peer_lost);
}

// Checks emitted events in case we unpeer from a remote peer.
CAF_TEST(unpeer_core1_from_core2) {
  CAF_MESSAGE("initiate handshake between core1 and core2");
  anon_send(core1, atom::peer_v, core2);
  run();
  CAF_MESSAGE("unpeer core1 and core2");
  anon_send(core1, atom::unpeer_v, core2);
  run();
  BROKER_CHECK_LOG(es.poll(), sc::peer_added, sc::peer_removed);
  CAF_MESSAGE("unpeering again emits peer_invalid");
  anon_send(core1, atom::unpeer_v, core2);
  run();
  BROKER_CHECK_LOG(es.poll(), ec::peer_invalid);
  CAF_MESSAGE("unpeering from an unconnected network emits peer_invalid");
  anon_send(core1, atom::unpeer_v, network_info{"localhost", 8080});
  run();
  BROKER_CHECK_LOG(es.poll(), ec::peer_invalid);
  anon_send_exit(core1, caf::exit_reason::user_shutdown);
  anon_send_exit(core2, caf::exit_reason::user_shutdown);
}

// Checks emitted events in case a remote peer unpeers.
CAF_TEST(unpeer_core2_from_core1) {
  // Initiate handshake between core1 and core2.
  anon_send(core2, atom::peer_v, core1);
  run();
  anon_send(core2, atom::unpeer_v, core1);
  run();
  BROKER_CHECK_LOG(es.poll(), sc::peer_added, sc::peer_lost);
  // Try unpeering again, this time on core1.
  anon_send(core1, atom::unpeer_v, core2);
  run();
  BROKER_CHECK_LOG(es.poll(), ec::peer_invalid);
  // Try unpeering from an unconnected network address.
  anon_send(core1, atom::unpeer_v, network_info{"localhost", 8080});
  run();
  BROKER_CHECK_LOG(es.poll(), ec::peer_invalid);
  anon_send_exit(core1, caf::exit_reason::user_shutdown);
  anon_send_exit(core2, caf::exit_reason::user_shutdown);
}

CAF_TEST_FIXTURE_SCOPE_END()

CAF_TEST_FIXTURE_SCOPE(distributed_peers, point_to_point_fixture<base_fixture>)

// Setup: driver -> earth.core -> mars.core -> leaf
CAF_TEST(remote_peers_setup1) {
  // --- phase 1: get state from fixtures and initialize cores -----------------
  auto core1 = native(earth.ep.core());
  auto core2 = native(mars.ep.core());
  caf::anon_send(core1, atom::no_events_v);
  caf::anon_send(core2, atom::no_events_v);
  caf::anon_send(core1, atom::subscribe_v, filter_type{"a", "b", "c"});
  caf::anon_send(core2, atom::subscribe_v, filter_type{"a", "b", "c"});
  exec_all();
  // --- phase 2: connect earth and mars at CAF level --------------------------
  // Prepare publish and remote_actor calls.
  CAF_MESSAGE("prepare connections on earth and mars");
  prepare_connection(mars, earth, "mars", 8080u);
  // Run any initialization code.
  exec_all();
  // Tell mars to listen for peers.
  CAF_MESSAGE("publish core on mars");
  mars.sched.inline_next_enqueue(); // listen() calls middleman().publish()
  auto res = mars.ep.listen("", 8080u);
  CAF_CHECK_EQUAL(res, 8080u);
  exec_all();
  // Establish connection between mars and earth before peering in order to
  // connect the streaming parts of CAF before we go into Broker code.
  CAF_MESSAGE("connect mars and earth");
  auto core2_proxy = earth.remote_actor("mars", 8080u);
  exec_all();
  // --- phase 4: spawn a leaf/consumer on mars and connect it to core2 --------
  // Connect a consumer (leaf) to core2.
  auto leaf = mars.sys.spawn(consumer, filter_type{"b"}, core2);
  CAF_MESSAGE("core1: " << to_string(core1));
  CAF_MESSAGE("core2: " << to_string(core2));
  CAF_MESSAGE("leaf: " << to_string(leaf));
  exec_all();
  // --- phase 5: peer from earth to mars --------------------------------------
  // Initiate handshake between core1 and core2.
  earth.self->send(core1, atom::peer_v, core2_proxy);
  exec_all();
  // Spin up driver on core1.
  auto d1 = earth.sys.spawn(driver, core1, false);
  CAF_MESSAGE("d1: " << to_string(d1));
  exec_all();
  // Check log of the consumer.
  using buf = std::vector<data_message>;
  earth.self->send(leaf, atom::get_v);
  exec_all();
  earth.self->receive([](const buf& xs) {
    auto expected =
      data_msgs({{"b", true}, {"b", false}, {"b", true}, {"b", false}});
    CAF_REQUIRE_EQUAL(xs, expected);
  });
  anon_send_exit(core1, caf::exit_reason::user_shutdown);
  anon_send_exit(core2, caf::exit_reason::user_shutdown);
  anon_send_exit(leaf, caf::exit_reason::user_shutdown);
  exec_all();
}

// Setup: driver -> mars.core -> earth.core -> leaf
CAF_TEST(remote_peers_setup2) {
  // --- phase 1: get state from fixtures and initialize cores -----------------
  auto core1 = native(earth.ep.core());
  auto core2 = native(mars.ep.core());
  caf::anon_send(core1, atom::no_events_v);
  caf::anon_send(core2, atom::no_events_v);
  caf::anon_send(core1, atom::subscribe_v, filter_type{"a", "b", "c"});
  caf::anon_send(core2, atom::subscribe_v, filter_type{"a", "b", "c"});
  exec_all();
  // --- phase 2: connect earth and mars at CAF level --------------------------
  // Prepare publish and remote_actor calls.
  CAF_MESSAGE("prepare connections on earth and mars");
  prepare_connection(mars, earth, "mars", 8080u);
  // Run any initialization code.
  exec_all();
  // Tell mars to listen for peers.
  CAF_MESSAGE("publish core on mars");
  mars.sched.inline_next_enqueue(); // listen() calls middleman().publish()
  auto res = mars.ep.listen("", 8080u);
  CAF_CHECK_EQUAL(res, 8080u);
  exec_all();
  // Establish connection between mars and earth before peering in order to
  // connect the streaming parts of CAF before we go into Broker code.
  CAF_MESSAGE("connect mars and earth");
  auto core2_proxy = earth.remote_actor("mars", 8080u);
  exec_all();
  // --- phase 4: spawn a leaf/consumer on earth and connect it to core2 -------
  // Connect a consumer (leaf) to core2.
  auto leaf = earth.sys.spawn(consumer, filter_type{"b"}, core1);
  CAF_MESSAGE("core1: " << to_string(core1));
  CAF_MESSAGE("core2: " << to_string(core2));
  CAF_MESSAGE("leaf: " << to_string(leaf));
  exec_all();
  // --- phase 5: peer from earth to mars --------------------------------------
  // Initiate handshake between core1 and core2.
  earth.self->send(core1, atom::peer_v, core2_proxy);
  exec_all();
  // Spin up driver on core2. Data flows from driver to core2 to core1 and
  // finally to leaf.
  auto d1 = mars.sys.spawn(driver, core2, false);
  CAF_MESSAGE("d1: " << to_string(d1));
  exec_all();
  // Check log of the consumer.
  using buf = std::vector<data_message>;
  mars.self->send(leaf, atom::get_v);
  mars.sched.prioritize(leaf);
  mars.consume_message();
  mars.self->receive([](const buf& xs) {
    auto expected =
      data_msgs({{"b", true}, {"b", false}, {"b", true}, {"b", false}});
    CAF_REQUIRE_EQUAL(xs, expected);
  });
  CAF_MESSAGE("shutdown core actors");
  anon_send_exit(core1, caf::exit_reason::user_shutdown);
  anon_send_exit(core2, caf::exit_reason::user_shutdown);
  anon_send_exit(leaf, caf::exit_reason::user_shutdown);
  exec_all();
}
