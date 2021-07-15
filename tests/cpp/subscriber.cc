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

#include "broker/atoms.hh"
#include "broker/configuration.hh"
#include "broker/convert.hh"
#include "broker/core_actor.hh"
#include "broker/data.hh"
#include "broker/endpoint.hh"
#include "broker/filter_type.hh"
#include "broker/message.hh"
#include "broker/topic.hh"

using std::cout;
using std::endl;
using std::string;

using namespace caf;
using namespace broker;
using namespace broker::detail;

namespace {

struct fixture : base_fixture {
  // Returns the core manager for given actor.
  auto& state(caf::actor hdl) {
    return deref<core_actor_type>(hdl).state;
  }

  std::vector<data_message> test_data;

  fixture() {
    core1 = ep.core();
    core2 = sys.spawn<core_actor_type>(ids['A'], filter_type{"a", "b", "c"});
    anon_send(core1, atom::no_events_v);
    anon_send(core2, atom::no_events_v);
    test_data = data_msgs({{"a", 0},
                           {"b", true},
                           {"a", 1},
                           {"a", 2},
                           {"b", false},
                           {"b", true},
                           {"a", 3},
                           {"b", false},
                           {"a", 4},
                           {"a", 5}});
    run();
  }

  ~fixture() {
    anon_send_exit(core1, exit_reason::user_shutdown);
    anon_send_exit(core2, exit_reason::user_shutdown);
  }

  caf::actor core1;
  caf::actor core2;
};

} // namespace <anonymous>

FIXTURE_SCOPE(subscriber_tests, fixture)

TEST(blocking_subscriber) {
  MESSAGE("subscribe to data on core1");
  sched.inline_next_enqueue();
  auto sub = ep.make_subscriber(filter_type{"b"});
  run();
  MESSAGE("establish peering relation between the cores");
  bridge(core1, core2);
  run();
  MESSAGE("publish data on core2");
  push_data(core2, test_data);
  run();
  CAF_MESSAGE("check content of the subscriber's buffer");
  using buf = std::vector<data_message>;
  auto expected = data_msgs({{"b", true}, {"b", false},
                             {"b", true}, {"b", false}});
  CHECK_EQUAL(sub.available(), 4u);
  CHECK_EQUAL(sub.poll(), expected);
}

TEST(nonblocking_subscriber) {
  MESSAGE("subscribe to data on core1");
  using buf = std::vector<data_message>;
  buf result;
  ep.subscribe_nosync(
    {"b"},
    [](unit_t&) {
      // nop
    },
    [&](unit_t&, data_message x) {
      result.emplace_back(std::move(x));
    },
    [](unit_t&, const error&) {
      // nop
    }
  );
  MESSAGE("establish peering relation between the cores");
  bridge(core1, core2);
  run();
  MESSAGE("publish data on core2");
  push_data(core2, test_data);
  run();
  MESSAGE("check content of the subscriber's buffer");
  auto expected = data_msgs({{"b", true}, {"b", false},
                             {"b", true}, {"b", false}});
  CHECK_EQUAL(result, expected);
}

FIXTURE_SCOPE_END()
