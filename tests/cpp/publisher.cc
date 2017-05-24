// This unit test is a variation of the `core` unit test that uses a
// `publisher` instead of an event-based `driver` actor.

#define CAF_SUITE publisher
#include "test.hpp"
#include <caf/test/dsl.hpp>

#include "broker/broker.hh"

#include "broker/detail/core_actor.hh"
#include "broker/detail/filter_type.hh"

using std::cout;
using std::endl;
using std::string;

using namespace broker;
using namespace broker::detail;

using namespace caf;

using filter_type = ::broker::detail::filter_type;
using value_type = std::pair<topic, data>;
using stream_type = stream<std::pair<topic, data>>;

namespace {

struct consumer_state {
  std::vector<value_type> xs;
};

behavior consumer(stateful_actor<consumer_state>* self,
                  filter_type ts, const actor& src) {
  self->send(self * src, atom::join::value, std::move(ts));
  return {
    [=](const stream_type& in) {
      self->add_sink(
        // Input stream.
        in,
        // Initialize state.
        [](unit_t&) {
          // nop
        },
        // Process single element.
        [=](unit_t&, value_type x) {
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

} // namespace <anonymous>

CAF_TEST_FIXTURE_SCOPE(publisher_tests, base_fixture)

CAF_TEST(blocking_publishers) {
  // Spawn/get/configure core actors.
  auto core1 = ep.core();
  auto core2 = sys.spawn(core_actor, filter_type{"a", "b", "c"});
  anon_send(core1, atom::subscribe::value, filter_type{"a", "b", "c"});
  sched.run();
  // Connect a consumer (leaf) to core2.
  auto leaf = sys.spawn(consumer, filter_type{"b"}, core2);
  sched.run_once();
  expect((atom_value, filter_type),
         from(leaf).to(core2).with(join_atom::value, filter_type{"b"}));
  expect((stream_msg::open), from(_).to(leaf).with(_, core2, _, _, false));
  expect((stream_msg::ack_open), from(leaf).to(core2).with(_, 5, _, false));
  // Initiate handshake between core1 and core2.
  self->send(core1, atom::peer::value, core2);
  expect((atom::peer, actor), from(self).to(core1).with(_, core2));
  // Step #1: core1  --->    ('peer', filter_type)    ---> core2
  expect((atom::peer, filter_type, actor),
         from(core1).to(core2).with(_, filter_type{"a", "b", "c"}, core1));
  // Step #2: core1  <---   (stream_msg::open)   <--- core2
  expect((stream_msg::open),
         from(_).to(core1).with(
           std::make_tuple(_, filter_type{"a", "b", "c"}), core2, _, _,
           false));
  // Step #3: core1  --->   (stream_msg::open)   ---> core2
  //          core1  ---> (stream_msg::ack_open) ---> core2
  expect((stream_msg::open), from(_).to(core2).with(_, core1, _, _, false));
  expect((stream_msg::ack_open), from(core1).to(core2).with(_, 5, _, false));
  expect((stream_msg::ack_open), from(core2).to(core1).with(_, 5, _, false));
  { // Lifetime scope of our publishers.
    // There must be no communication pending at this point.
    CAF_REQUIRE(!sched.has_job());
    // Spin up two publishers: one for "a" and one for "b".
    publisher pub1{ep, "a"};
    auto d1 = pub1.worker();
    sched.run_once();
    expect((stream_msg::open), from(_).to(core1).with(_, d1, _, _, false));
    expect((stream_msg::ack_open), from(core1).to(d1).with(_, 5, _, false));
    CAF_REQUIRE_EQUAL(pub1.demand(), 10); // 5 demand + 5 extra buffer
    publisher pub2{ep, "b"};
    auto d2 = pub2.worker();
    sched.run_once();
    expect((stream_msg::open), from(_).to(core1).with(_, d2, _, _, false));
    expect((stream_msg::ack_open), from(core1).to(d2).with(_, 5, _, false));
    CAF_REQUIRE_EQUAL(pub2.demand(), 10); // 5 demand + 5 extra buffer
    // Data flows from our publishers to core1 to core2 and finally to leaf.
    using buf = std::vector<value_type>;
    // First set of published messages gets filtered out at core2.
    pub1.publish(0);
    expect((atom_value), from(_).to(d1).with(atom::resume::value));
    expect((stream_msg::batch), from(d1).to(core1).with(1, _, 0));
    expect((stream_msg::batch), from(core1).to(core2).with(1, _, 0));
    expect((stream_msg::ack_batch), from(core2).to(core1).with(1, 0));
    expect((stream_msg::ack_batch), from(core1).to(d1).with(1, 0));
    // Must not be forwarded to `leaf`.
    CAF_REQUIRE(!sched.has_job());
    // Second set of published messages gets delivered to leaf.
    pub2.publish(true);
    expect((atom_value), from(_).to(d2).with(atom::resume::value));
    expect((stream_msg::batch), from(d2).to(core1).with(1, _, 0));
    expect((stream_msg::batch), from(core1).to(core2).with(1, _, 1));
    expect((stream_msg::batch), from(core2).to(leaf).with(1, _, 0));
    expect((stream_msg::ack_batch), from(leaf).to(core2).with(1, 0));
    expect((stream_msg::ack_batch), from(core2).to(core1).with(1, 1));
    expect((stream_msg::ack_batch), from(core1).to(d2).with(1, 0));
    // Third set of published messages gets again filtered out at core2.
    pub1.publish({1, 2, 3});
    expect((atom_value), from(_).to(d1).with(atom::resume::value));
    expect((stream_msg::batch), from(d1).to(core1).with(3, _, 1));
    expect((stream_msg::batch), from(core1).to(core2).with(3, _, 2));
    expect((stream_msg::ack_batch), from(core2).to(core1).with(3, 2));
    expect((stream_msg::ack_batch), from(core1).to(d1).with(3, 1));
    // Fourth set of published messages gets delivered to leaf again.
    pub2.publish({false, true});
    expect((atom_value), from(_).to(d2).with(atom::resume::value));
    expect((stream_msg::batch), from(d2).to(core1).with(2, _, 1));
    expect((stream_msg::batch), from(core1).to(core2).with(2, _, 3));
    expect((stream_msg::batch), from(core2).to(leaf).with(2, _, 1));
    expect((stream_msg::ack_batch), from(leaf).to(core2).with(2, 1));
    expect((stream_msg::ack_batch), from(core2).to(core1).with(2, 3));
    expect((stream_msg::ack_batch), from(core1).to(d2).with(2, 1));
    // Check log of the consumer.
    self->send(leaf, atom::get::value);
    sched.prioritize(leaf);
    sched.run_once();
    self->receive(
      [](const buf& xs) {
      buf expected{{"b", true}, {"b", false}, {"b", true}};
      CAF_REQUIRE_EQUAL(xs, expected);
      }
      );
  }
  // Shutdown.
  CAF_MESSAGE("Shutdown core actors.");
  anon_send_exit(core1, exit_reason::user_shutdown);
  anon_send_exit(core2, exit_reason::user_shutdown);
  anon_send_exit(leaf, exit_reason::user_shutdown);
  sched.run();
}

CAF_TEST(nonblocking_publishers) {
  // Spawn/get/configure core actors.
  auto core1 = ep.core();
  auto core2 = sys.spawn(core_actor, filter_type{"a", "b", "c"});
  anon_send(core1, atom::subscribe::value, filter_type{"a", "b", "c"});
  sched.run();
  // Connect a consumer (leaf) to core2.
  auto leaf = sys.spawn(consumer, filter_type{"b"}, core2);
  sched.run_once();
  expect((atom_value, filter_type),
         from(leaf).to(core2).with(join_atom::value, filter_type{"b"}));
  expect((stream_msg::open), from(_).to(leaf).with(_, core2, _, _, false));
  expect((stream_msg::ack_open), from(leaf).to(core2).with(_, 5, _, false));
  // Initiate handshake between core1 and core2.
  self->send(core1, atom::peer::value, core2);
  expect((atom::peer, actor), from(self).to(core1).with(_, core2));
  // Step #1: core1  --->    ('peer', filter_type)    ---> core2
  expect((atom::peer, filter_type, actor),
         from(core1).to(core2).with(_, filter_type{"a", "b", "c"}, core1));
  // Step #2: core1  <---   (stream_msg::open)   <--- core2
  expect((stream_msg::open),
         from(_).to(core1).with(std::make_tuple(_, filter_type{"a", "b", "c"}),
                                core2, _, _, false));
  // Step #3: core1  --->   (stream_msg::open)   ---> core2
  //          core1  ---> (stream_msg::ack_open) ---> core2
  expect((stream_msg::open), from(_).to(core2).with(_, core1, _, _, false));
  expect((stream_msg::ack_open), from(core1).to(core2).with(_, 5, _, false));
  expect((stream_msg::ack_open), from(core2).to(core1).with(_, 5, _, false));
  // There must be no communication pending at this point.
  CAF_REQUIRE(!sched.has_job());
  // publish_all uses thread communication which would deadlock when using our
  // test_scheduler. We avoid this by pushing the call to publish_all to its
  // own thread.
  using buf_type = std::vector<value_type>;
  ep.publish_all_nosync(
    // Initialize send buffer with 10 elements.
    [](buf_type& xs) {
      xs = buf_type{{"a", 0}, {"b", true}, {"a", 1}, {"a", 2}, {"b", false},
                    {"b", true}, {"a", 3}, {"b", false}, {"a", 4}, {"a", 5}};
    },
    // Get next element.
    [](buf_type& xs, downstream<value_type>& out, size_t num) {
      auto n = std::min(num, xs.size());
      for (size_t i = 0u; i < n; ++i)
        out.push(xs[i]);
      xs.erase(xs.begin(), xs.begin() + static_cast<ptrdiff_t>(n));
    },
    // Did we reach the end?.
    [](const buf_type& xs) {
      return xs.empty();
    },
    // Handle result of the stream.
    [](expected<void>) {
      // nop
    }
  );
  // Communication is identical to the driver-driven test in test/cpp/core.cc
  sched.run();
  // Check log of the consumer.
  self->send(leaf, atom::get::value);
  sched.prioritize(leaf);
  sched.run_once();
  self->receive(
    [](const buf_type& xs) {
      buf_type expected{{"b", true}, {"b", false}, {"b", true}, {"b", false}};
      CAF_REQUIRE_EQUAL(xs, expected);
    }
  );
  // Shutdown.
  CAF_MESSAGE("Shutdown core actors.");
  anon_send_exit(core1, exit_reason::user_shutdown);
  anon_send_exit(core2, exit_reason::user_shutdown);
  anon_send_exit(leaf, exit_reason::user_shutdown);
  sched.run();
}

CAF_TEST_FIXTURE_SCOPE_END()
