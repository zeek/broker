// This unit test is a variation of the `core` unit test that uses a
// `subscriber` instead of an event-based `consumer` actor.

#define CAF_SUITE subscriber
#include "test.hpp"
#include <caf/test/dsl.hpp>

#include "broker/broker.hh"

#include "broker/detail/core_actor.hh"
#include "broker/detail/filter_type.hh"

using std::cout;
using std::endl;
using std::string;

using namespace caf;
using namespace broker;
using namespace broker::detail;

using value_type = std::pair<topic, data>;

namespace {

void driver(event_based_actor* self, const actor& sink) {
  using buf_type = std::vector<value_type>;
  self->new_stream(
    // Destination.
    sink,
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
}

} // namespace <anonymous>

CAF_TEST_FIXTURE_SCOPE(subscriber_tests, base_fixture)

CAF_TEST(blocking_subscriber) {
  // Spawn/get/configure core actors.
  auto core1 = sys.spawn(core_actor, filter_type{"a", "b", "c"});
  auto core2 = ep.core();
  anon_send(core2, atom::subscribe::value, filter_type{"a", "b", "c"});
  sched.run();
  // Connect a consumer (leaf) to core2.
  // auto leaf = sys.spawn(consumer, filter_type{"b"}, core2);
  subscriber sub{ep, filter_type{"b"}};
  auto leaf = sub.worker();
  sched.run_once();
  expect((atom_value, filter_type),
         from(leaf).to(core2).with(join_atom::value, filter_type{"b"}));
  expect((stream_msg::open), from(_).to(leaf).with(_, core2, _, _, false));
  expect((stream_msg::ack_open), from(leaf).to(core2).with(_, 20, _, false));
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
  // There must be no communication pending at this point.
  CAF_REQUIRE(!sched.has_job());
  // Spin up driver on core1.
  auto d1 = sys.spawn(driver, core1);
  sched.run_once();
  expect((stream_msg::open), from(_).to(core1).with(_, d1, _, _, false));
  expect((stream_msg::ack_open), from(core1).to(d1).with(_, 5, _, false));
  // Data flows from driver to core1 to core2 and finally to leaf.
  expect((stream_msg::batch), from(d1).to(core1).with(5, _, 0));
  expect((stream_msg::batch), from(core1).to(core2).with(5, _, 0));
  expect((stream_msg::batch), from(core2).to(leaf).with(2, _, 0));
  expect((stream_msg::ack_batch), from(core2).to(core1).with(5, 0));
  expect((stream_msg::ack_batch), from(core1).to(d1).with(5, 0));
  CAF_MESSAGE("check content of the subscriber's buffer");
  auto x0 = sub.get();
  CAF_REQUIRE_EQUAL(x0.first, "b");
  CAF_REQUIRE_EQUAL(x0.second, true);
  auto xs = sub.poll();
  CAF_REQUIRE_EQUAL(xs.size(), 1);
  CAF_REQUIRE_EQUAL(xs[0].first, "b");
  CAF_REQUIRE_EQUAL(xs[0].second, false);
  // Shutdown.
  CAF_MESSAGE("Shutdown core actors.");
  anon_send_exit(core1, exit_reason::user_shutdown);
  anon_send_exit(core2, exit_reason::user_shutdown);
  anon_send_exit(leaf, exit_reason::user_shutdown);
  sched.run();
  sched.inline_next_enqueues(std::numeric_limits<size_t>::max());
}

CAF_TEST(nonblocking_subscriber) {
  // Spawn/get/configure core actors.
  auto core1 = sys.spawn(core_actor, filter_type{"a", "b", "c"});
  auto core2 = ep.core();
  anon_send(core2, atom::subscribe::value, filter_type{"a", "b", "c"});
  sched.run();
  // Initiate handshake between core1 and core2.
  self->send(core1, atom::peer::value, core2);
  expect((atom::peer, actor), from(self).to(core1).with(_, core2));
  // Connect a subscriber (leaf) to core2.
  using buf = std::vector<value_type>;
  buf result;
  ep.subscribe_nosync(
    {"b"},
    [](unit_t&) {
      // nop
    },
    [&](unit_t&, value_type x) {
      result.emplace_back(std::move(x));
    },
    [](unit_t&) {
      // nop
    }
  );
  // Spin up driver on core1.
  auto d1 = sys.spawn(driver, core1);
  // Communication is identical to the consumer-centric test in test/cpp/core.cc
  sched.run();
  buf expected{{"b", true}, {"b", false}, {"b", true}, {"b", false}};
  CAF_REQUIRE_EQUAL(result, expected);
  // Shutdown.
  CAF_MESSAGE("Shutdown core actors.");
  anon_send_exit(core1, exit_reason::user_shutdown);
  anon_send_exit(core2, exit_reason::user_shutdown);
  sched.run();
  sched.inline_next_enqueues(std::numeric_limits<size_t>::max());
}

CAF_TEST_FIXTURE_SCOPE_END()
