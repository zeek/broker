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
#include "broker/envelope.hh"
#include "broker/filter_type.hh"
#include "broker/internal/core_actor.hh"
#include "broker/internal/native.hh"
#include "broker/internal/type_id.hh"
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
  std::vector<data_envelope_ptr> out_buf;

  std::vector<std::string> out_topics;
  std::vector<data> out_data;

  fixture()
    : out_buf{
      data_envelope::make("foo", 0),     data_envelope::make("foo", true),
      data_envelope::make("foo", 1),     data_envelope::make("foo", 2),
      data_envelope::make("foo", false), data_envelope::make("foo", true),
      data_envelope::make("foo", 3),     data_envelope::make("foo", false),
      data_envelope::make("foo", 4),     data_envelope::make("foo", 5)} {
    for (auto& msg : out_buf) {
      out_topics.emplace_back(msg->topic());
      out_data.emplace_back(msg->value().to_data());
    }
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
  std::vector<std::string> in_topics;
  std::vector<data> in_data;
  for (auto& msg : inputs) {
    in_topics.emplace_back(msg->topic());
    in_data.emplace_back(msg->value().to_data());
  }
  CHECK_EQUAL(earth_sub.available(), 0u);
  CHECK_EQUAL(inputs.size(), 10u);
  CHECK_EQUAL(in_topics, out_topics);
  CHECK_EQUAL(in_data, out_data);
}

FIXTURE_SCOPE_END()
