#define SUITE core
#include "test.hpp"
#include <caf/test/io_dsl.hpp>

#include "broker/endpoint.hh"
#include "broker/detail/core_actor.hh"

using namespace caf;
using namespace broker;
using namespace broker::detail;

using element_type = endpoint::stream_type::value_type;

namespace {

using make_restartable_atom = caf::atom_constant<caf::atom("mkRstrtbl")>;
using restart_atom = caf::atom_constant<caf::atom("restart")>;

struct driver_state {
  using buf_type = std::vector<element_type>;
  bool restartable = false;
  buf_type xs;
  static const char* name;
  void reset() {
    xs
      = buf_type{{"a", 0},    {"b", true}, {"a", 1},     {"a", 2}, {"b", false},
                 {"b", true}, {"a", 3},    {"b", false}, {"a", 4}, {"a", 5}};
  }
  driver_state() {
    reset();
  }
};

const char* driver_state::name = "driver";

behavior driver(stateful_actor<driver_state>* self, const actor& sink) {
  auto ptr = self->new_stream(
    // Destination.
    sink,
    // Initialize send buffer with 10 elements.
    [](unit_t&) {
      // nop
    },
    // Get next element.
    [=](unit_t&, downstream<element_type>& out, size_t num) {
      auto& xs = self->state.xs;
      auto n = std::min(num, xs.size());
      if (n == 0)
        return;
      for (size_t i = 0u; i < n; ++i)
        out.push(xs[i]);
      xs.erase(xs.begin(), xs.begin() + static_cast<ptrdiff_t>(n));
    },
    // Did we reach the end?.
    [=](const unit_t&) {
      auto& st = self->state;
      return !st.restartable && st.xs.empty();
    },
    // Handle result of the stream.
    [](expected<void>) {
      // nop
    }
  ).ptr();
  return {
    [=](make_restartable_atom) {
      self->state.restartable = true;
    },
    [=](restart_atom) {
      self->state.reset();
      self->state.restartable = false;
      static_cast<stream_source*>(ptr.get())->generate();
      ptr->push();
    }
  };
}

struct consumer_state {
  std::vector<element_type> xs;
  static const char* name;
};

const char* consumer_state::name = "consumer";

behavior consumer(stateful_actor<consumer_state>* self, filter_type ts,
                  const actor& src) {
  self->send(self * src, atom::join::value, std::move(ts));
  return {
    [=](const endpoint::stream_type& in) {
      self->add_sink(
        // Input stream.
        in,
        // Initialize state.
        [](unit_t&) {
          // nop
        },
        // Process single element.
        [=](unit_t&, element_type x) {
          self->state.xs.emplace_back(std::move(x));
        },
        // Cleanup.
        [](unit_t&) {
          // nop
        }
      );
    },
    [=](atom::get) {
      return self->state.xs;
    }
  };
}

struct config : actor_system_config {
public:
  config() {
    add_message_type<element_type>("element");
  }
};

using fixture = test_coordinator_fixture<config>;

} // namespace <anonymous>

CAF_TEST_FIXTURE_SCOPE(local_tests, fixture)

// Simulates a simple setup with two cores, where data flows from core1 to
// core2.
CAF_TEST(local_peers) {
  // Spawn core actors and disable events.
  auto core1 = sys.spawn(core_actor, filter_type{"a", "b", "c"});
  auto core2 = sys.spawn(core_actor, filter_type{"a", "b", "c"});
  anon_send(core1, atom::no_events::value);
  anon_send(core2, atom::no_events::value);
  sched.run();
  // Connect a consumer (leaf) to core2.
  auto leaf = sys.spawn(consumer, filter_type{"b"}, core2);
  CAF_MESSAGE("core1: " << to_string(core1));
  CAF_MESSAGE("core2: " << to_string(core2));
  CAF_MESSAGE("leaf: " << to_string(leaf));
  sched.run_once();
  expect((atom_value, filter_type),
         from(leaf).to(core2).with(join_atom::value, filter_type{"b"}));
  expect((stream_msg::open), from(_).to(leaf).with(_, core2, _, _, false));
  expect((stream_msg::ack_open), from(leaf).to(core2).with(_, 5, _, false));
  // Initiate handshake between core1 and core2.
  self->send(core1, atom::peer::value, core2);
  expect((atom::peer, actor), from(self).to(core1).with(_, core2));
  // Check if core1 reports a pending peer.
  CAF_MESSAGE("query peer information from core1");
  sched.inline_next_enqueue();
  self->request(core1, infinite, atom::get::value, atom::peer::value).receive(
    [&](const std::vector<peer_info>& xs) {
      CAF_REQUIRE_EQUAL(xs.size(), 1);
      CAF_REQUIRE_EQUAL(xs.front().status, peer_status::connecting);
    },
    [&](const error& err) {
      CAF_FAIL(sys.render(err));
    }
  );
  // Step #1: core1  --->    ('peer', filter_type)    ---> core2
  expect((atom::peer, filter_type, actor),
         from(core1).to(core2).with(_, filter_type{"a", "b", "c"}, core1));
  // Step #2: core1  <---   (stream_msg::open)   <--- core2
  expect((stream_msg::open),
         from(_).to(core1).with(
           std::make_tuple(_, filter_type{"a", "b", "c"}, core2), core2, _, _,
           false));
  // Step #3: core1  --->   (stream_msg::open)   ---> core2
  //          core1  ---> (stream_msg::ack_open) ---> core2
  expect((stream_msg::open), from(_).to(core2).with(_, core1, _, _, false));
  expect((stream_msg::ack_open), from(core1).to(core2).with(_, 5, _, false));
  expect((stream_msg::ack_open), from(core2).to(core1).with(_, 5, _, false));
  // There must be no communication pending at this point.
  CAF_REQUIRE(!sched.has_job());
  // Check if core1 & core2 both report each other as peered.
  CAF_MESSAGE("query peer information from core1");
  sched.inline_next_enqueue();
  self->request(core1, infinite, atom::get::value, atom::peer::value).receive(
    [&](const std::vector<peer_info>& xs) {
      CAF_REQUIRE_EQUAL(xs.size(), 1);
      CAF_REQUIRE_EQUAL(xs.front().status, peer_status::peered);
    },
    [&](const error& err) {
      CAF_FAIL(sys.render(err));
    }
  );
  CAF_MESSAGE("query peer information from core2");
  sched.inline_next_enqueue();
  self->request(core2, infinite, atom::get::value, atom::peer::value).receive(
    [&](const std::vector<peer_info>& xs) {
      CAF_REQUIRE_EQUAL(xs.size(), 1);
      CAF_REQUIRE_EQUAL(xs.front().status, peer_status::peered);
    },
    [&](const error& err) {
      CAF_FAIL(sys.render(err));
    }
  );
  // Spin up driver on core1.
  auto d1 = sys.spawn(driver, core1);
  CAF_MESSAGE("d1: " << to_string(d1));
  sched.run_once();
  expect((stream_msg::open), from(_).to(core1).with(_, d1, _, _, false));
  expect((stream_msg::ack_open), from(core1).to(d1).with(_, 5, _, false));
  // Data flows from driver to core1 to core2 and finally to leaf.
  using buf = std::vector<element_type>;
  expect((stream_msg::batch), from(d1).to(core1).with(5, _, 0));
  expect((stream_msg::batch), from(core1).to(core2).with(5, _, 0));
  expect((stream_msg::batch), from(core2).to(leaf).with(2, _, 0));
  expect((stream_msg::ack_batch), from(core2).to(core1).with(5, 0));
  expect((stream_msg::ack_batch), from(core1).to(d1).with(5, 0));
  // Check log of the consumer.
  self->send(leaf, atom::get::value);
  sched.prioritize(leaf);
  sched.run_once();
  self->receive(
    [](const buf& xs) {
      buf expected{{"b", true}, {"b", false}};
      CAF_REQUIRE_EQUAL(xs, expected);
    }
  );
  CAF_SET_LOGGER_SYS(&sys);
  CAF_LOG_INFO("deliver remaining items from driver");
  CAF_MESSAGE("deliver remaining items from driver");
  sched.run();
  // Check log of the consumer after receiving all items from driver.
  self->send(leaf, atom::get::value);
  sched.prioritize(leaf);
  sched.run_once();
  self->receive(
    [](const buf& xs) {
      buf expected{{"b", true}, {"b", false}, {"b", true}, {"b", false}};
      CAF_REQUIRE_EQUAL(xs, expected);
    }
  );
  // Send a message "directly" from core1 to core2 by bypassing the stream.
  anon_send(core1, atom::publish::value, endpoint_info{core2.node(), caf::none},
            topic("b"), data{true});
  expect((atom::publish, endpoint_info, topic, data),
         from(_).to(core1).with(_, _, _, _));
  expect((atom::publish, atom::local, topic, data),
         from(core1).to(core2).with(_, _, topic("b"), data{true}));
  expect((stream_msg::batch), from(core2).to(leaf).with(1, _, _));
  // Check log of the consumer one last time.
  self->send(leaf, atom::get::value);
  sched.prioritize(leaf);
  sched.run_once();
  self->receive(
    [](const buf& xs) {
      buf expected{{"b", true}, {"b", false}, {"b", true},
                   {"b", false}, {"b", true}};
      CAF_REQUIRE_EQUAL(xs, expected);
    }
  );
  // Tell core1 to unpeer from core2
  CAF_MESSAGE("unpeer core1 from core2");
  anon_send(core1, atom::unpeer::value, core2);
  sched.run();
  // Check if core1 and core2 reports no more peers.
  CAF_MESSAGE("query peer information from core1");
  sched.inline_next_enqueue();
  self->request(core1, infinite, atom::get::value, atom::peer::value).receive(
    [&](const std::vector<peer_info>& xs) {
      CAF_REQUIRE_EQUAL(xs.size(), 0);
    },
    [&](const error& err) {
      CAF_FAIL(sys.render(err));
    }
  );
  sched.inline_next_enqueue();
  self->request(core2, infinite, atom::get::value, atom::peer::value).receive(
    [&](const std::vector<peer_info>& xs) {
      CAF_REQUIRE_EQUAL(xs.size(), 0);
    },
    [&](const error& err) {
      CAF_FAIL(sys.render(err));
    }
  );
  // Shutdown.
  CAF_MESSAGE("Shutdown core actors.");
  anon_send_exit(core1, exit_reason::user_shutdown);
  anon_send_exit(core2, exit_reason::user_shutdown);
  anon_send_exit(leaf, exit_reason::user_shutdown);
  sched.run();
  sched.inline_next_enqueues(std::numeric_limits<size_t>::max());
}

// Simulates a simple triance setup where core1 peers with core2, and core2
// peers with core3. Data flows from core1 to core2 and core3.
CAF_TEST(triangle_peering) {
  // Spawn core actors and disable events.
  auto core1 = sys.spawn(core_actor, filter_type{"a", "b", "c"});
  auto core2 = sys.spawn(core_actor, filter_type{"a", "b", "c"});
  auto core3 = sys.spawn(core_actor, filter_type{"a", "b", "c"});
  anon_send(core1, atom::no_events::value);
  anon_send(core2, atom::no_events::value);
  anon_send(core3, atom::no_events::value);
  sched.run();
  // Connect a consumer (leaf) to core1 (this consumer never receives data,
  // because data isn't forwarded to local subscribers).
  auto leaf1 = sys.spawn(consumer, filter_type{"b"}, core1);
  // Connect a consumer (leaf) to core2.
  auto leaf2 = sys.spawn(consumer, filter_type{"b"}, core2);
  sched.run();
  // Connect a consumer (leaf) to core3.
  auto leaf3 = sys.spawn(consumer, filter_type{"b"}, core3);
  sched.run();
  // Initiate handshake between core1 and core2.
  self->send(core1, atom::peer::value, core2);
  expect((atom::peer, actor), from(self).to(core1).with(_, core2));
  // Check if core1 reports a pending peer.
  CAF_MESSAGE("query peer information from core1");
  sched.inline_next_enqueue();
  self->request(core1, infinite, atom::get::value, atom::peer::value).receive(
    [&](const std::vector<peer_info>& xs) {
      CAF_REQUIRE_EQUAL(xs.size(), 1);
      CAF_REQUIRE_EQUAL(xs.front().status, peer_status::connecting);
    },
    [&](const error& err) {
      CAF_FAIL(sys.render(err));
    }
  );
  // Step #1: core1  --->    ('peer', filter_type)    ---> core2
  expect((atom::peer, filter_type, actor),
         from(core1).to(core2).with(_, filter_type{"a", "b", "c"}, core1));
  // Step #2: core1  <---   (stream_msg::open)   <--- core2
  expect((stream_msg::open),
         from(_).to(core1).with(
           std::make_tuple(_, filter_type{"a", "b", "c"}, core2), core2, _, _,
           false));
  // Step #3: core1  --->   (stream_msg::open)   ---> core2
  //          core1  ---> (stream_msg::ack_open) ---> core2
  expect((stream_msg::open), from(_).to(core2).with(_, core1, _, _, false));
  expect((stream_msg::ack_open), from(core1).to(core2).with(_, 5, _, false));
  expect((stream_msg::ack_open), from(core2).to(core1).with(_, 5, _, false));
  // There must be no communication pending at this point.
  CAF_REQUIRE(!sched.has_job());
  // Initiate handshake between core2 and core3.
  self->send(core2, atom::peer::value, core3);
  expect((atom::peer, actor), from(self).to(core2).with(_, core3));
  // Check if core2 reports a pending peer.
  CAF_MESSAGE("query peer information from core2");
  sched.inline_next_enqueue();
  self->request(core2, infinite, atom::get::value, atom::peer::value).receive(
    [&](const std::vector<peer_info>& xs) {
      CAF_REQUIRE_EQUAL(xs.size(), 2);
      CAF_REQUIRE_EQUAL(xs.front().status, peer_status::peered);
      CAF_REQUIRE_EQUAL(xs.back().status, peer_status::connecting);
    },
    [&](const error& err) {
      CAF_FAIL(sys.render(err));
    }
  );
  // Step #1: core2  --->    ('peer', filter_type)    ---> core3
  expect((atom::peer, filter_type, actor),
         from(core2).to(core3).with(_, filter_type{"a", "b", "c"}, core2));
  // Step #2: core2  <---   (stream_msg::open)   <--- core3
  expect((stream_msg::open),
         from(_).to(core2).with(
           std::make_tuple(_, filter_type{"a", "b", "c"}, core3), core3, _, _,
           false));
  // Step #3: core2  --->   (stream_msg::open)   ---> core3
  //          core2  ---> (stream_msg::ack_open) ---> core3
  expect((stream_msg::open), from(_).to(core3).with(_, core2, _, _, false));
  expect((stream_msg::ack_open), from(core2).to(core3).with(_, 5, _, false));
  expect((stream_msg::ack_open), from(core3).to(core2).with(_, 5, _, false));
  // There must be no communication pending at this point.
  CAF_REQUIRE(!sched.has_job());
  // Check if all cores properly report peering setup.
  CAF_MESSAGE("query peer information from core1");
  sched.inline_next_enqueue();
  self->request(core1, infinite, atom::get::value, atom::peer::value).receive(
    [&](const std::vector<peer_info>& xs) {
      CAF_REQUIRE_EQUAL(xs.size(), 1);
      CAF_REQUIRE_EQUAL(xs.front().status, peer_status::peered);
    },
    [&](const error& err) {
      CAF_FAIL(sys.render(err));
    }
  );
  CAF_MESSAGE("query peer information from core2");
  sched.inline_next_enqueue();
  self->request(core2, infinite, atom::get::value, atom::peer::value).receive(
    [&](const std::vector<peer_info>& xs) {
      CAF_REQUIRE_EQUAL(xs.size(), 2);
      CAF_REQUIRE_EQUAL(xs.front().status, peer_status::peered);
      CAF_REQUIRE_EQUAL(xs.back().status, peer_status::peered);
    },
    [&](const error& err) {
      CAF_FAIL(sys.render(err));
    }
  );
  CAF_MESSAGE("query peer information from core3");
  sched.inline_next_enqueue();
  self->request(core3, infinite, atom::get::value, atom::peer::value).receive(
    [&](const std::vector<peer_info>& xs) {
      CAF_REQUIRE_EQUAL(xs.size(), 1);
      CAF_REQUIRE_EQUAL(xs.front().status, peer_status::peered);
    },
    [&](const error& err) {
      CAF_FAIL(sys.render(err));
    }
  );
  // Spin up driver on core1.
  auto d1 = sys.spawn(driver, core1);
  CAF_MESSAGE("d1: " << to_string(d1));
  sched.run();
  // Check log of the consumers.
  using buf = std::vector<element_type>;
  buf expected{{"b", true}, {"b", false}, {"b", true}, {"b", false}};
  for (auto& leaf : {leaf2, leaf3}) {
    self->send(leaf, atom::get::value);
    sched.prioritize(leaf);
    sched.run_once();
    self->receive(
      [&](const buf& xs) {
        CAF_REQUIRE_EQUAL(xs, expected);
      }
    );
  }
  // Make sure leaf1 never received any data.
  self->send(leaf1, atom::get::value);
  sched.prioritize(leaf1);
  sched.run_once();
  self->receive(
    [&](const buf& xs) {
      CAF_REQUIRE(xs.empty());
    }
  );
  // Shutdown.
  CAF_MESSAGE("Shutdown core actors.");
  anon_send_exit(core1, exit_reason::user_shutdown);
  anon_send_exit(core2, exit_reason::user_shutdown);
  anon_send_exit(core3, exit_reason::user_shutdown);
  sched.run();
  sched.inline_next_enqueues(std::numeric_limits<size_t>::max());
}

// Simulates a simple setup where core1 peers with core2 and starts sending
// data. After receiving a couple of messages, core2 terminates and core3
// starts peering. Core3 must receive all remaining messages.
// peers with core3. Data flows from core1 to core2 and core3.
CAF_TEST(sequenced_peering) {
  // Spawn core actors and disable events.
  auto core1 = sys.spawn(core_actor, filter_type{"a", "b", "c"});
  auto core2 = sys.spawn(core_actor, filter_type{"a", "b", "c"});
  auto core3 = sys.spawn(core_actor, filter_type{"a", "b", "c"});
  anon_send(core1, atom::no_events::value);
  anon_send(core2, atom::no_events::value);
  anon_send(core3, atom::no_events::value);
  sched.run();
  // Connect a consumer (leaf) to core2.
  auto leaf1 = sys.spawn(consumer, filter_type{"b"}, core2);
  sched.run();
  // Connect a consumer (leaf) to core3.
  auto leaf2 = sys.spawn(consumer, filter_type{"b"}, core3);
  sched.run();
  // Initiate handshake between core1 and core2.
  self->send(core1, atom::peer::value, core2);
  expect((atom::peer, actor), from(self).to(core1).with(_, core2));
  // Check if core1 reports a pending peer.
  CAF_MESSAGE("query peer information from core1");
  sched.inline_next_enqueue();
  self->request(core1, infinite, atom::get::value, atom::peer::value).receive(
    [&](const std::vector<peer_info>& xs) {
      CAF_REQUIRE_EQUAL(xs.size(), 1);
      CAF_REQUIRE_EQUAL(xs.front().status, peer_status::connecting);
    },
    [&](const error& err) {
      CAF_FAIL(sys.render(err));
    }
  );
  // Step #1: core1  --->    ('peer', filter_type)    ---> core2
  expect((atom::peer, filter_type, actor),
         from(core1).to(core2).with(_, filter_type{"a", "b", "c"}, core1));
  // Step #2: core1  <---   (stream_msg::open)   <--- core2
  expect((stream_msg::open),
         from(_).to(core1).with(
           std::make_tuple(_, filter_type{"a", "b", "c"}, core2), core2, _, _,
           false));
  // Step #3: core1  --->   (stream_msg::open)   ---> core2
  //          core1  ---> (stream_msg::ack_open) ---> core2
  expect((stream_msg::open), from(_).to(core2).with(_, core1, _, _, false));
  expect((stream_msg::ack_open), from(core1).to(core2).with(_, 5, _, false));
  expect((stream_msg::ack_open), from(core2).to(core1).with(_, 5, _, false));
  // There must be no communication pending at this point.
  CAF_REQUIRE(!sched.has_job());
  // Spin up driver and transmit first half of the data.
  auto d1 = sys.spawn(driver, core1);
  anon_send(d1, make_restartable_atom::value);
  CAF_MESSAGE("d1: " << to_string(d1));
  sched.run();
  // Check log of the consumer on core2.
  using buf = std::vector<element_type>;
  buf expected{{"b", true}, {"b", false}, {"b", true}, {"b", false}};
  self->send(leaf1, atom::get::value);
  sched.prioritize(leaf1);
  sched.run_once();
  self->receive(
    [&](const buf& xs) {
      CAF_REQUIRE_EQUAL(xs, expected);
    }
  );
  CAF_MESSAGE("kill core2");
  anon_send_exit(core2, exit_reason::user_shutdown);
  sched.run();
  CAF_MESSAGE("make sure core1 sees no peer anymore");
  sched.inline_next_enqueue();
  self->request(core1, infinite, atom::get::value, atom::peer::value).receive(
    [&](const std::vector<peer_info>& xs) {
      CAF_REQUIRE_EQUAL(xs.size(), 0);
    },
    [&](const error& err) {
      CAF_FAIL(sys.render(err));
    }
  );
  // Initiate handshake between core1 and core3.
  self->send(core1, atom::peer::value, core3);
  expect((atom::peer, actor), from(self).to(core1).with(_, core3));
  // Check if core1 reports a pending peer.
  CAF_MESSAGE("query peer information from core1");
  sched.inline_next_enqueue();
  self->request(core1, infinite, atom::get::value, atom::peer::value).receive(
    [&](const std::vector<peer_info>& xs) {
      CAF_REQUIRE_EQUAL(xs.size(), 1);
      CAF_REQUIRE_EQUAL(xs.front().status, peer_status::connecting);
    },
    [&](const error& err) {
      CAF_FAIL(sys.render(err));
    }
  );
  // Step #1: core1  --->    ('peer', filter_type)    ---> core3
  expect((atom::peer, filter_type, actor),
         from(core1).to(core3).with(_, filter_type{"a", "b", "c"}, core1));
  // Step #2: core1  <---   (stream_msg::open)   <--- core3
  expect((stream_msg::open),
         from(_).to(core1).with(
           std::make_tuple(_, filter_type{"a", "b", "c"}, core3), core3, _, _,
           false));
  // Step #3: core1  --->   (stream_msg::open)   ---> core3
  //          core1  ---> (stream_msg::ack_open) ---> core3
  expect((stream_msg::open), from(_).to(core3).with(_, core1, _, _, false));
  expect((stream_msg::ack_open), from(core1).to(core3).with(_, 5, _, false));
  expect((stream_msg::ack_open), from(core3).to(core1).with(_, 5, _, false));
  // There must be no communication pending at this point.
  CAF_REQUIRE(!sched.has_job());
  // Restart driver and send second half of the data.
  anon_send(d1, restart_atom::value);
  sched.run();
  // Check log of the consumer on core3.
  self->send(leaf2, atom::get::value);
  sched.prioritize(leaf2);
  sched.run_once();
  self->receive(
    [&](const buf& xs) {
      CAF_CHECK_EQUAL(xs, expected);
    }
  );
  // Shutdown.
  CAF_MESSAGE("Shutdown core actors.");
  anon_send_exit(core1, exit_reason::user_shutdown);
  anon_send_exit(core3, exit_reason::user_shutdown);
  sched.run();
  sched.inline_next_enqueues(std::numeric_limits<size_t>::max());
}

CAF_TEST_FIXTURE_SCOPE_END()

namespace {

struct error_signaling_fixture : base_fixture {
  actor core1;
  actor core2;
  event_subscriber es;

  error_signaling_fixture() : es(ep.make_event_subscriber(true)) {
    core1 = ep.core();
    anon_send(core1, atom::subscribe::value, filter_type{"a", "b", "c"});
    core2 = sys.spawn(core_actor, filter_type{"a", "b", "c"});
    anon_send(core2, atom::no_events::value);
    sched.run();
  }

  ~error_signaling_fixture() {
    sched.inline_next_enqueues(std::numeric_limits<size_t>::max());
  }
};

struct event_visitor {
  using result_type = caf::variant<caf::none_t, sc, ec>;
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

} // namespace <anonymous>

CAF_TEST_FIXTURE_SCOPE(error_signaling, error_signaling_fixture)

// Simulates a connection abort after sending 'peer' message ("stage #0").
CAF_TEST(failed_handshake_stage0) {
  // Spawn core actors and disable events.
  // Initiate handshake between core1 and core2, but kill core2 right away.
  self->send(core1, atom::peer::value, core2);
  anon_send_exit(core2, exit_reason::kill);
  expect((atom::peer, actor), from(self).to(core1).with(_, core2));
  sched.run();
  BROKER_CHECK_LOG(es.poll(), ec::peer_unavailable);
}

// Simulates a connection abort after receiving stage #1 handshake.
CAF_TEST(failed_handshake_stage1) {
  // Initiate handshake between core1 and core2, but kill core2 right away.
  self->send(core2, atom::peer::value, core1);
  expect((atom::peer, actor), from(self).to(core2).with(_, core1));
  expect((atom::peer, filter_type, actor),
         from(core2).to(core1).with(_, filter_type{"a", "b", "c"}, core2));
  anon_send_exit(core2, exit_reason::kill);
  sched.run();
  BROKER_CHECK_LOG(es.poll(), sc::peer_added, sc::peer_lost);
}

// Simulates a connection abort after sending 'peer' message ("stage #0").
CAF_TEST(failed_handshake_stage2) {
  // Spawn core actors and disable events.
  // Initiate handshake between core1 and core2, but kill core2 right away.
  self->send(core1, atom::peer::value, core2);
  expect((atom::peer, actor), from(self).to(core1).with(_, core2));
  expect((atom::peer, filter_type, actor),
         from(_).to(core2).with(_, filter_type{"a", "b", "c"}, core1));
  anon_send_exit(core2, exit_reason::kill);
  expect((stream_msg::open), from(_).to(core1).with(_, _, _, _, _, false));
  sched.run();
  BROKER_CHECK_LOG(es.poll(), sc::peer_added, sc::peer_lost);
}

// Simulates a connection abort after receiving stage #1 handshake.
CAF_TEST(failed_handshake_stage3) {
  // Initiate handshake between core1 and core2, but kill core2 right away.
  self->send(core2, atom::peer::value, core1);
  expect((atom::peer, actor), from(self).to(core2).with(_, core1));
  expect((atom::peer, filter_type, actor),
         from(core2).to(core1).with(_, filter_type{"a", "b", "c"}, core2));
  expect((stream_msg::open), from(_).to(core2).with(_, core1, _, _, false));
  anon_send_exit(core2, exit_reason::kill);
  expect((stream_msg::open), from(_).to(core1).with(_, core2, _, _, false));
  sched.run();
  BROKER_CHECK_LOG(es.poll(), sc::peer_added, sc::peer_lost);
}

// Checks emitted events in case we unpeer from a remote peer.
CAF_TEST(unpeer_core1_from_core2) {
  // Initiate handshake between core1 and core2.
  anon_send(core1, atom::peer::value, core2);
  sched.run();
  anon_send(core1, atom::unpeer::value, core2);
  sched.run();
  BROKER_CHECK_LOG(es.poll(), sc::peer_added, sc::peer_removed);
  // Try unpeering again and check if we receive a `peer_invalid` error.
  anon_send(core1, atom::unpeer::value, core2);
  sched.run();
  BROKER_CHECK_LOG(es.poll(), ec::peer_invalid);
  // Try unpeering from an unconnected network address.
  anon_send(core1, atom::unpeer::value, network_info{"localhost", 8080});
  sched.run();
  BROKER_CHECK_LOG(es.poll(), ec::peer_invalid);
}

// Checks emitted events in case a remote peer unpeers.
CAF_TEST(unpeer_core2_from_core1) {
  // Initiate handshake between core1 and core2.
  anon_send(core2, atom::peer::value, core1);
  sched.run();
  anon_send(core2, atom::unpeer::value, core1);
  sched.run();
  BROKER_CHECK_LOG(es.poll(), sc::peer_added, sc::peer_lost);
  // Try unpeering again, this time on core1.
  anon_send(core1, atom::unpeer::value, core2);
  sched.run();
  BROKER_CHECK_LOG(es.poll(), ec::peer_invalid);
  // Try unpeering from an unconnected network address.
  anon_send(core1, atom::unpeer::value, network_info{"localhost", 8080});
  sched.run();
  BROKER_CHECK_LOG(es.poll(), ec::peer_invalid);
}

CAF_TEST_FIXTURE_SCOPE_END()

CAF_TEST_FIXTURE_SCOPE(distributed_peers,
                       point_to_point_fixture<fake_network_fixture>)

// Setup: driver -> earth.core -> mars.core -> leaf
CAF_TEST(remote_peers_setup1) {
  // --- phase 1: get state from fixtures and initialize cores -----------------
  auto core1 = earth.ep.core();
  auto ss1 = earth.stream_serv; 
  auto core2 = mars.ep.core();
  auto ss2 = mars.stream_serv;
  auto forward_stream_traffic = [&] {
    auto exec_ss = [&](fake_network_fixture& ff, const strong_actor_ptr& ss) {
      if (ff.sched.prioritize(ss)) {
        do {
          ff.sched.run_once();
        } while (ff.sched.prioritize(ss1));
        return true;
      }
      return false;
    };
    while (earth.mpx.try_exec_runnable() || mars.mpx.try_exec_runnable()
           || earth.mpx.read_data() || mars.mpx.read_data()
           || exec_ss(earth, ss1) || exec_ss(mars, ss2)) {
      // rince and repeat
    }
  };
  anon_send(core1, atom::no_events::value);
  anon_send(core2, atom::no_events::value);
  anon_send(core1, atom::subscribe::value, filter_type{"a", "b", "c"});
  anon_send(core2, atom::subscribe::value, filter_type{"a", "b", "c"});
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
  // --- phase 3: initialize streaming-related state in CAF --------------------
  // Establish remote paths between the two stream servers. This step is
  // necessary to prevent a deadlock in `stream_serv::remote_stream_serv` due
  // to blocking communication with the BASP broker.
  CAF_MESSAGE("connect streaming worker of mars and earth");
  anon_send(actor_cast<actor>(ss1), connect_atom::value, ss2->node());
  anon_send(actor_cast<actor>(ss2), connect_atom::value, ss1->node());
  exec_all();
  // --- phase 4: spawn a leaf/consumer on mars and connect it to core2 --------
  // Connect a consumer (leaf) to core2.
  auto leaf = mars.sys.spawn(consumer, filter_type{"b"}, core2);
  CAF_MESSAGE("core1: " << to_string(core1));
  CAF_MESSAGE("core2: " << to_string(core2));
  CAF_MESSAGE("leaf: " << to_string(leaf));
  mars.sched.run_once();
  expect_on(mars, (atom_value, filter_type),
            from(leaf).to(core2).with(join_atom::value, filter_type{"b"}));
  expect_on(mars, (stream_msg::open),
            from(_).to(leaf).with(_, core2, _, _, false));
  expect_on(mars, (stream_msg::ack_open),
            from(leaf).to(core2).with(_, 5, _, false));
  // --- phase 5: peer from earth to mars --------------------------------------
  // Initiate handshake between core1 and core2.
  earth.self->send(core1, atom::peer::value, core2_proxy);
  expect_on(earth, (atom::peer, actor),
            from(earth.self).to(core1).with(_, core2_proxy));
  // Step #1: core1  --->    ('peer', filter_type)    ---> core2
  forward_stream_traffic();
  expect_on(mars, (atom::peer, filter_type, actor),
            from(_).to(core2).with(_, filter_type{"a", "b", "c"}, _));
  // Step #2: core1  <---   (stream_msg::open)   <--- core2
  forward_stream_traffic();
  expect_on(earth, (stream_msg::open),
            from(_).to(core1).with(
              std::make_tuple(_, filter_type{"a", "b", "c"}, core2_proxy), _, _,
              _, false));
  // Step #3: core1  --->   (stream_msg::open)   ---> core2
  //          core1  ---> (stream_msg::ack_open) ---> core2
  forward_stream_traffic();
  expect_on(mars, (stream_msg::open),
            from(_).to(core2).with(_, _, _, _, false));
  expect_on(mars, (stream_msg::ack_open),
            from(_).to(core2).with(_, 5, _, false));
  // Step #4: core1  <--- (stream_msg::ack_open) <--- core2
  forward_stream_traffic();
  expect_on(earth, (stream_msg::ack_open),
            from(_).to(core1).with(_, 5, _, false));
  // Make sure there is no communication pending at this point.
  exec_all();
  // Spin up driver on core1.
  auto d1 = earth.sys.spawn(driver, core1);
  CAF_MESSAGE("d1: " << to_string(d1));
  earth.sched.run_once();
  expect_on(earth, (stream_msg::open),
            from(_).to(core1).with(_, d1, _, _, false));
  expect_on(earth, (stream_msg::ack_open),
            from(core1).to(d1).with(_, 5, _, false));
  // Data flows from driver to core1 to core2 and finally to leaf.
  using buf = std::vector<element_type>;
  expect_on(earth, (stream_msg::batch), from(d1).to(core1).with(5, _, 0));
  forward_stream_traffic();
  expect_on(mars, (stream_msg::batch), from(_).to(core2).with(5, _, 0));
  expect_on(mars, (stream_msg::batch), from(core2).to(leaf).with(2, _, 0));
  expect_on(mars, (stream_msg::ack_batch), from(leaf).to(core2).with(2, 0));
  forward_stream_traffic();
  expect_on(earth, (stream_msg::ack_batch), from(_).to(core1).with(5, 0));
  expect_on(earth, (stream_msg::ack_batch), from(_).to(d1).with(5, 0));
  // Second round of data.
  expect_on(earth, (stream_msg::batch), from(d1).to(core1).with(5, _, 1));
  forward_stream_traffic();
  expect_on(mars, (stream_msg::batch), from(_).to(core2).with(5, _, 1));
  expect_on(mars, (stream_msg::batch),
            from(core2).to(leaf).with(2, _, 1));
  expect_on(mars, (stream_msg::ack_batch), from(leaf).to(core2).with(2, 1));
  forward_stream_traffic();
  expect_on(earth, (stream_msg::ack_batch), from(_).to(core1).with(5, 1));
  expect_on(earth, (stream_msg::ack_batch), from(_).to(d1).with(5, 1));
  expect_on(earth, (stream_msg::close), from(d1).to(core1).with());
  // Check log of the consumer.
  earth.self->send(leaf, atom::get::value);
  earth.sched.prioritize(leaf);
  earth.sched.run_once();
  earth.self->receive(
    [](const buf& xs) {
      buf expected{{"b", true}, {"b", false}, {"b", true}, {"b", false}};
      CAF_REQUIRE_EQUAL(xs, expected);
    }
  );
  anon_send_exit(core1, exit_reason::user_shutdown);
  anon_send_exit(core2, exit_reason::user_shutdown);
  anon_send_exit(leaf, exit_reason::user_shutdown);
  exec_all();
  mars.sched.inline_next_enqueues(std::numeric_limits<size_t>::max());
  earth.sched.inline_next_enqueues(std::numeric_limits<size_t>::max());
}

// Setup: driver -> mars.core -> earth.core -> leaf
CAF_TEST(remote_peers_setup2) {
  // --- phase 1: get state from fixtures and initialize cores -----------------
  auto core1 = earth.ep.core();
  auto ss1 = earth.stream_serv; 
  auto core2 = mars.ep.core();
  auto ss2 = mars.stream_serv;
  auto forward_stream_traffic = [&] {
    auto exec_ss = [&](fake_network_fixture& ff, const strong_actor_ptr& ss) {
      if (ff.sched.prioritize(ss)) {
        do {
          ff.sched.run_once();
        } while (ff.sched.prioritize(ss1));
        return true;
      }
      return false;
    };
    while (earth.mpx.try_exec_runnable() || mars.mpx.try_exec_runnable()
           || earth.mpx.read_data() || mars.mpx.read_data()
           || exec_ss(earth, ss1) || exec_ss(mars, ss2)) {
      // rince and repeat
    }
  };
  anon_send(core1, atom::no_events::value);
  anon_send(core2, atom::no_events::value);
  anon_send(core1, atom::subscribe::value, filter_type{"a", "b", "c"});
  anon_send(core2, atom::subscribe::value, filter_type{"a", "b", "c"});
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
  // --- phase 3: initialize streaming-related state in CAF --------------------
  // Establish remote paths between the two stream servers. This step is
  // necessary to prevent a deadlock in `stream_serv::remote_stream_serv` due
  // to blocking communication with the BASP broker.
  CAF_MESSAGE("connect streaming worker of mars and earth");
  anon_send(actor_cast<actor>(ss1), connect_atom::value, ss2->node());
  anon_send(actor_cast<actor>(ss2), connect_atom::value, ss1->node());
  exec_all();
  // --- phase 4: spawn a leaf/consumer on earth and connect it to core2 -------
  // Connect a consumer (leaf) to core2.
  auto leaf = earth.sys.spawn(consumer, filter_type{"b"}, core1);
  CAF_MESSAGE("core1: " << to_string(core1));
  CAF_MESSAGE("core2: " << to_string(core2));
  CAF_MESSAGE("leaf: " << to_string(leaf));
  earth.sched.run_once();
  expect_on(earth, (atom_value, filter_type),
            from(leaf).to(core1).with(join_atom::value, filter_type{"b"}));
  expect_on(earth, (stream_msg::open),
            from(_).to(leaf).with(_, core1, _, _, false));
  expect_on(earth, (stream_msg::ack_open),
            from(leaf).to(core1).with(_, 5, _, false));
  // --- phase 5: peer from earth to mars --------------------------------------
  // Initiate handshake between core1 and core2.
  earth.self->send(core1, atom::peer::value, core2_proxy);
  expect_on(earth, (atom::peer, actor),
            from(earth.self).to(core1).with(_, core2_proxy));
  // Step #1: core1  --->    ('peer', filter_type)    ---> core2
  forward_stream_traffic();
  expect_on(mars, (atom::peer, filter_type, actor),
            from(_).to(core2).with(_, filter_type{"a", "b", "c"}, _));
  // Step #2: core1  <---   (stream_msg::open)   <--- core2
  forward_stream_traffic();
  expect_on(earth, (stream_msg::open),
            from(_).to(core1).with(
              std::make_tuple(_, filter_type{"a", "b", "c"}, core2_proxy), _, _,
              _, false));
  // Step #3: core1  --->   (stream_msg::open)   ---> core2
  //          core1  ---> (stream_msg::ack_open) ---> core2
  forward_stream_traffic();
  expect_on(mars, (stream_msg::open),
            from(_).to(core2).with(_, _, _, _, false));
  expect_on(mars, (stream_msg::ack_open),
            from(_).to(core2).with(_, 5, _, false));
  // Step #4: core1  <--- (stream_msg::ack_open) <--- core2
  forward_stream_traffic();
  expect_on(earth, (stream_msg::ack_open),
            from(_).to(core1).with(_, 5, _, false));
  // Make sure there is no communication pending at this point.
  exec_all();
  // Spin up driver on core2.
  auto d1 = mars.sys.spawn(driver, core2);
  CAF_MESSAGE("d1: " << to_string(d1));
  mars.sched.run_once();
  expect_on(mars, (stream_msg::open),
            from(_).to(core2).with(_, d1, _, _, false));
  expect_on(mars, (stream_msg::ack_open),
            from(core2).to(d1).with(_, 5, _, false));
  // Data flows from driver to core2 to core1 and finally to leaf.
  using buf = std::vector<element_type>;
  expect_on(mars, (stream_msg::batch), from(d1).to(core2).with(5, _, 0));
  forward_stream_traffic();
  expect_on(earth, (stream_msg::batch), from(_).to(core1).with(5, _, 0));
  expect_on(earth, (stream_msg::batch),
            from(core1).to(leaf).with(2, _, 0));
  expect_on(earth, (stream_msg::ack_batch), from(leaf).to(core1).with(2, 0));
  forward_stream_traffic();
  expect_on(mars, (stream_msg::ack_batch), from(_).to(core2).with(5, 0));
  expect_on(mars, (stream_msg::ack_batch), from(_).to(d1).with(5, 0));
  // Second round of data.
  expect_on(mars, (stream_msg::batch), from(d1).to(core2).with(5, _, 1));
  forward_stream_traffic();
  expect_on(earth, (stream_msg::batch), from(_).to(core1).with(5, _, 1));
  expect_on(earth, (stream_msg::batch), from(core1).to(leaf).with(2, _, 1));
  expect_on(earth, (stream_msg::ack_batch), from(leaf).to(core1).with(2, 1));
  forward_stream_traffic();
  expect_on(mars, (stream_msg::ack_batch), from(_).to(core2).with(5, 1));
  expect_on(mars, (stream_msg::ack_batch), from(_).to(d1).with(5, 1));
  expect_on(mars, (stream_msg::close), from(d1).to(core2).with());
  // Check log of the consumer.
  mars.self->send(leaf, atom::get::value);
  mars.sched.prioritize(leaf);
  mars.sched.run_once();
  mars.self->receive(
    [](const buf& xs) {
      buf expected{{"b", true}, {"b", false}, {"b", true}, {"b", false}};
      CAF_REQUIRE_EQUAL(xs, expected);
    }
  );
  CAF_MESSAGE("shutdown core actors");
  anon_send_exit(core1, exit_reason::user_shutdown);
  anon_send_exit(core2, exit_reason::user_shutdown);
  anon_send_exit(leaf, exit_reason::user_shutdown);
  exec_all();
  mars.sched.inline_next_enqueues(std::numeric_limits<size_t>::max());
  earth.sched.inline_next_enqueues(std::numeric_limits<size_t>::max());
}

CAF_TEST_FIXTURE_SCOPE_END()
