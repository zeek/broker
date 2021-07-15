#define SUITE status_subscriber

#include "broker/status_subscriber.hh"

#include "test.hh"

#include <iostream>
#include <string>
#include <utility>

#include <caf/exit_reason.hpp>
#include <caf/group.hpp>
#include <caf/send.hpp>

#include "broker/atoms.hh"
#include "broker/endpoint.hh"
#include "broker/error.hh"
#include "broker/status.hh"

using std::cout;
using std::endl;
using std::string;

using namespace broker;
using namespace broker::detail;

namespace {

struct fixture : base_fixture {
  fixture() {
    node = endpoint_id::random(0x5EED);
    node_str = to_string(node);
  }

  void push(error x) {
    data xs;
    if (!convert(x, xs))
      FAIL("unable to convert error to data");
    caf::anon_send(ep.core(), atom::publish_v, atom::local_v,
                   make_data_message(topics::errors, std::move(xs)));
  }

  void push(status x) {
    data xs;
    if (!convert(x, xs))
      FAIL("unable to convert status to data");
    caf::anon_send(ep.core(), atom::publish_v, atom::local_v,
                   make_data_message(topics::statuses, std::move(xs)));
  }

  endpoint_id node;

  std::string node_str;
};

} // namespace <anonymous>

CAF_TEST_FIXTURE_SCOPE(status_subscriber_tests, fixture)

CAF_TEST(base_tests) {
  run();
  sched.inline_next_enqueue();
  auto sub1 = ep.make_status_subscriber(true);
  run();
  sched.inline_next_enqueue();
  auto sub2 = ep.make_status_subscriber(false);
  run();
  CAF_REQUIRE_EQUAL(sub1.available(), 0u);
  CAF_REQUIRE_EQUAL(sub2.available(), 0u);
  CAF_MESSAGE("test error event");
  error e1 = ec::type_clash;
  push(e1);
  run();
  CAF_REQUIRE_EQUAL(sub1.available(), 1u);
  CAF_REQUIRE_EQUAL(sub1.get(), e1);
  CAF_REQUIRE_EQUAL(sub2.available(), 1u);
  CAF_REQUIRE_EQUAL(sub2.get(), e1);
  CAF_MESSAGE("test status event");
  auto s1 = status::make<sc::endpoint_discovered>(node, "foobar");
  push(s1);
  run();
  CAF_REQUIRE_EQUAL(sub1.available(), 1u);
  CAF_REQUIRE_EQUAL(sub1.get(), s1);
  CAF_REQUIRE_EQUAL(sub2.available(), 0u);
  CAF_MESSAGE("shutdown");
  anon_send_exit(ep.core(), caf::exit_reason::user_shutdown);
}

CAF_TEST_FIXTURE_SCOPE_END()
