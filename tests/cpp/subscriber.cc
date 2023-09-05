// This unit test is a variation of the `core` unit test that uses a
// `subscriber` instead of an event-based `consumer` actor.

#define SUITE subscriber

#include "broker/subscriber.hh"

#include "test.hh"

#include <caf/actor.hpp>
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

struct fixture : net_fixture<base_fixture> {
  std::vector<data_message> out_buf;

  fixture()
    : out_buf{make_data_message("foo", 0),     make_data_message("foo", true),
              make_data_message("foo", 1),     make_data_message("foo", 2),
              make_data_message("foo", false), make_data_message("foo", true),
              make_data_message("foo", 3),     make_data_message("foo", false),
              make_data_message("foo", 4),     make_data_message("foo", 5)} {
    // nop
  }
};

} // namespace

FIXTURE_SCOPE(subscriber_tests, fixture)

TEST(subscribers receive data from remote publications) {
  MESSAGE("subscribe to 'foo' on mars and earth");
  auto mars_sub = mars.ep.make_subscriber({"foo"});
  auto earth_sub = earth.ep.make_subscriber({"foo"});
  run();
  MESSAGE("establish a peering between earth and mars");
  bridge(earth, mars);
  MESSAGE("publish events on mars");
  for (auto& msg : out_buf)
    mars.ep.publish(msg);
  run();
  MESSAGE("expect to see the events on earth but not on mars (origin)");
  CHECK_EQUAL(mars_sub.available(), 0u);
  CHECK_EQUAL(earth_sub.available(), 10u);
  auto inputs = earth_sub.poll();
  CHECK_EQUAL(earth_sub.available(), 0u);
  CHECK_EQUAL(inputs.size(), 10u);
  CHECK_EQUAL(inputs, out_buf);
}

FIXTURE_SCOPE_END()
