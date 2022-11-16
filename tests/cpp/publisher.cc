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
#include <caf/stateful_actor.hpp>

#include "broker/configuration.hh"
#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/endpoint.hh"
#include "broker/filter_type.hh"
#include "broker/internal/core_actor.hh"
#include "broker/internal/native.hh"
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

struct no_state {};

} // namespace

FIXTURE_SCOPE(publisher_tests, net_fixture<base_fixture>)

TEST(publishers make data available to remote subscribers) {
  MESSAGE("subscribe to 'foo' on mars and earth");
  auto earth_buf = std::make_shared<std::vector<data_message>>();
  auto earth_sub = earth.ep.subscribe(
    {"foo"},                                   // Topics.
    [](no_state&) {},                          // Init.
    [earth_buf](no_state&, data_message msg) { // OnNext.
      earth_buf->emplace_back(msg);
    },
    [](no_state&, const error&) {}); // Cleanup.
  auto mars_buf = std::make_shared<std::vector<data_message>>();
  auto mars_sub = mars.ep.subscribe(
    {"foo"},                                  // Topics.
    [](no_state&) {},                         // Init.
    [mars_buf](no_state&, data_message msg) { // OnNext.
      mars_buf->emplace_back(msg);
    },
    [](no_state&, const error&) {}); // Cleanup.
  run();
  MESSAGE("establish a peering between earth and mars");
  bridge(earth, mars);
  MESSAGE("publish 10 events on mars");
  auto pub = mars.ep.make_publisher("foo");
  run();
  auto initial_demand = pub.demand();
  CHECK_GREATER_EQUAL(initial_demand, 10u);
  CHECK_EQUAL(pub.buffered(), 0u);
  for (count i = 0; i < 10; ++i)
    pub.publish(data{i});
  CHECK_EQUAL(pub.buffered(), 10u);
  run();
  CHECK_EQUAL(pub.demand(), initial_demand);
  CHECK_EQUAL(pub.buffered(), 0u);
  MESSAGE("expect to see the events on earth but not on mars (origin)");
  CHECK_EQUAL(mars_buf->size(), 0u);
  CHECK_EQUAL(earth_buf->size(), 10u);
  MESSAGE("stop background workers");
  earth.ep.stop(earth_sub);
  mars.ep.stop(mars_sub);
}

FIXTURE_SCOPE_END()

// This regression test requires a non-deterministic setup since it checks that
// a publisher eventually becomes unblocked via background activities.
TEST(regression GH196) {
  MESSAGE("connect two endpoints over localhost");
  endpoint ep1;
  endpoint ep2;
  auto sub1 = ep1.make_subscriber({topic{"/test"}});
  auto sub2 = ep1.make_subscriber({topic{"/test"}});
  REQUIRE(ep1.await_filter_entry(topic{"/test"}));
  auto port = ep1.listen("127.0.0.1", 0);
  ep2.peer("127.0.0.1", port);
  auto pub = ep2.make_publisher({topic{"/test"}});
  MESSAGE("wait until the peers have finished the handshake");
  REQUIRE(ep1.await_peer(ep2.node_id()));
  REQUIRE(ep2.await_peer(ep1.node_id()));
  auto cap = pub.capacity();
  MESSAGE("publish data, cap: " << cap);
  std::vector<data> batch1;
  for (size_t i = 0; i < cap; ++i)
    batch1.emplace_back(i);
  auto batch2 = batch1;
  pub.publish(std::move(batch1));
  pub.publish(std::move(batch2));
  MESSAGE("receive data on sub1");
  for (size_t n = 0; n < 2; ++n) {
    for (size_t i = 0; i < cap; ++i) {
      auto msg = sub1.get();
      CHECK_EQUAL(get_topic(msg).string(), "/test");
      CHECK_EQUAL(get_data(msg), data(i));
    }
  }
  CHECK(sub1.poll().empty());
  MESSAGE("receive data on sub2");
  auto res = sub2.get(cap * 2);
  for (size_t n = 0; n < 2; ++n) {
    for (size_t i = 0; i < cap; ++i) {
      auto& msg = res[(n * cap) + i];
      CHECK_EQUAL(get_topic(msg).string(), "/test");
      CHECK_EQUAL(get_data(msg), data(i));
    }
  }
  CHECK(sub2.poll().empty());
}
