// This unit test is a variation of the `core` unit test that uses a
// `subscriber` instead of an event-based `consumer` actor.

#define SUITE subscriber

#include "broker/subscriber.hh"

#include "test.hh"

#include <caf/actor.hpp>
#include <caf/attach_stream_source.hpp>
#include <caf/downstream.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/exit_reason.hpp>
#include <caf/send.hpp>

#include "broker/configuration.hh"
#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/endpoint.hh"
#include "broker/filter_type.hh"
#include "broker/internal/core_actor.hh"
#include "broker/internal/native.hh"
#include "broker/internal/type_id.hh"
#include "broker/message.hh"
#include "broker/topic.hh"

using broker::internal::native;
using std::cout;
using std::endl;
using std::string;

namespace atom = broker::internal::atom;

using namespace broker;
using namespace broker::detail;

namespace {

void driver(caf::event_based_actor* self, const caf::actor& sink) {
  using buf_type = std::vector<data_message>;
  caf::attach_stream_source(
    self,
    // Destination.
    sink,
    // Initialize send buffer with 10 elements.
    [](buf_type& xs) {
      xs = data_msgs({{"a", 0},     {"b", true}, {"a", 1}, {"a", 2},
                      {"b", false}, {"b", true}, {"a", 3}, {"b", false},
                      {"a", 4},     {"a", 5}});
    },
    // Get next element.
    [](buf_type& xs, caf::downstream<data_message>& out, size_t num) {
      auto n = std::min(num, xs.size());
      for (size_t i = 0u; i < n; ++i)
        out.push(xs[i]);
      xs.erase(xs.begin(), xs.begin() + static_cast<ptrdiff_t>(n));
    },
    // Did we reach the end?.
    [](const buf_type& xs) { return xs.empty(); });
}

} // namespace <anonymous>

CAF_TEST_FIXTURE_SCOPE(subscriber_tests, base_fixture)

CAF_TEST(blocking_subscriber) {
  // Spawn/get/configure core actors.
  broker_options options;
  options.disable_ssl = true;
  auto core1 = sys.spawn<internal::core_actor_type>(filter_type{"a", "b", "c"},
                                                    options, nullptr);
  auto core2 = native(ep.core());
  caf::anon_send(core2, atom::subscribe_v, filter_type{"a", "b", "c"});
  caf::anon_send(core1, atom::no_events_v);
  caf::anon_send(core2, atom::no_events_v);
  run();
  // Connect a consumer (leaf) to core2.
  // auto leaf = sys.spawn(consumer, filter_type{"b"}, core2);
  auto sub = ep.make_subscriber(filter_type{"b"});
  sub.set_rate_calculation(false);
  auto leaf = native(sub.worker());
  CAF_MESSAGE("core1: " << to_string(core1));
  CAF_MESSAGE("core2: " << to_string(core2));
  CAF_MESSAGE("leaf: " << to_string(leaf));
  // Initiate handshake between core1 and core2.
  self->send(core1, atom::peer_v, core2);
  run();
  // Spin up driver on core1.
  auto d1 = sys.spawn(driver, core1);
  CAF_MESSAGE("driver: " << to_string(d1));
  run();
  CAF_MESSAGE("check content of the subscriber's buffer");
  using buf = std::vector<data_message>;
  auto expected = data_msgs({{"b", true}, {"b", false},
                             {"b", true}, {"b", false}});
  CAF_CHECK_EQUAL(sub.poll(), expected);
  // Shutdown.
  CAF_MESSAGE("Shutdown core actors.");
  caf::anon_send_exit(core1, caf::exit_reason::user_shutdown);
  caf::anon_send_exit(core2, caf::exit_reason::user_shutdown);
  caf::anon_send_exit(leaf, caf::exit_reason::user_shutdown);
  caf::anon_send_exit(d1, caf::exit_reason::user_shutdown);
}

CAF_TEST(nonblocking_subscriber) {
  // Spawn/get/configure core actors.
  broker_options options;
  options.disable_ssl = true;
  auto core1 = sys.spawn<internal::core_actor_type>(filter_type{"a", "b", "c"},
                                                    options, nullptr);
  auto core2 = native(ep.core());
  caf::anon_send(core1, atom::no_events_v);
  caf::anon_send(core2, atom::no_events_v);
  caf::anon_send(core2, atom::subscribe_v, filter_type{"a", "b", "c"});
  self->send(core1, atom::peer_v, core2);
  run();
  // Connect a subscriber (leaf) to core2.
  using buf = std::vector<data_message>;
  buf result;
  ep.subscribe_nosync(
    {"b"},
    [](caf::unit_t&) {
      // nop
    },
    [&](caf::unit_t&, data_message x) {
      result.emplace_back(std::move(x));
    },
    [](caf::unit_t&, const error&) {
      // nop
    }
  );
  // Spin up driver on core1.
  auto d1 = sys.spawn(driver, core1);
  // Communication is identical to the consumer-centric test in test/cpp/core.cc
  run();
  auto expected = data_msgs({{"b", true}, {"b", false},
                             {"b", true}, {"b", false}});
  CAF_REQUIRE_EQUAL(result, expected);
  // Shutdown.
  CAF_MESSAGE("Shutdown core actors.");
  caf::anon_send_exit(core1, caf::exit_reason::user_shutdown);
  caf::anon_send_exit(core2, caf::exit_reason::user_shutdown);
}

CAF_TEST_FIXTURE_SCOPE_END()
