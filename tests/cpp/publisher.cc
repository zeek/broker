// This unit test is a variation of the `core` unit test that uses a
// `publisher` instead of an event-based `driver` actor.

#define SUITE publisher

#include "broker/publisher.hh"

#include "test.hh"

#include <caf/actor.hpp>
#include <caf/behavior.hpp>
#include <caf/error.hpp>
#include <caf/exit_reason.hpp>
#include <caf/scoped_actor.hpp>
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

using namespace broker;

namespace {

using buf_type = std::vector<data_message>;

struct fixture : base_fixture {
  // Returns the core manager for given actor.
  auto& state(caf::actor hdl) {
    return deref<core_actor_type>(hdl).state;
  }

  fixture() {
    core1 = ep.core();
    core2 = sys.spawn<core_actor_type>(ids['A'], filter_type{"a"});
    anon_send(core1, atom::no_events_v);
    anon_send(core2, atom::no_events_v);
    run();
  }

  ~fixture() {
    caf::anon_send_exit(core1, caf::exit_reason::user_shutdown);
    caf::anon_send_exit(core2, caf::exit_reason::user_shutdown);
  }

  caf::actor core1;
  caf::actor core2;
};

} // namespace <anonymous>

CAF_TEST_FIXTURE_SCOPE(publisher_tests, fixture)

CAF_TEST(blocking_publishers) {
  auto buf = collect_data(core2, filter_type{"a/b"});
  run();
  MESSAGE("connect core1 to core2");
  bridge(core1, core2);
  run();
  MESSAGE("spin up two publishers, one for 'a' and one for 'a/b'");
  sched.inline_next_enqueue();
  auto pub1 = ep.make_publisher("a");
  sched.inline_next_enqueue();
  auto pub2 = ep.make_publisher("a/b");
  pub1.drop_all_on_destruction();
  pub2.drop_all_on_destruction();
  run();
  MESSAGE("publish data and check correct forwarding");
  // First, set of published messages gets filtered out at core2.
  pub1.publish(0);
  run();
  CHECK_EQUAL(buf->size(), 0u);
  // Second, set of published messages gets delivered to leaf.
  pub2.publish(true);
  run();
  CHECK_EQUAL(buf->size(), 1u);
  // Third, set of published messages gets again filtered out at core2.
  pub1.publish({1, 2, 3});
  run();
  CHECK_EQUAL(buf->size(), 1u);
  // Fourth, set of published messages gets delivered to leaf again.
  pub2.publish({false, true});
  run();
  CHECK_EQUAL(buf->size(), 3u);
  // Check log of the consumer.
  auto expected = data_msgs({{"a/b", true}, {"a/b", false}, {"a/b", true}});
  CAF_CHECK_EQUAL(*buf, expected);
}

CAF_TEST_FIXTURE_SCOPE_END()
