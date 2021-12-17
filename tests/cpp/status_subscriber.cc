#define SUITE status_subscriber

#include "broker/status_subscriber.hh"

#include "test.hh"

#include <iostream>
#include <string>
#include <utility>

#include <caf/exit_reason.hpp>
#include <caf/group.hpp>
#include <caf/send.hpp>

#include "broker/endpoint.hh"
#include "broker/error.hh"
#include "broker/internal/native.hh"
#include "broker/internal/type_id.hh"
#include "broker/status.hh"

using broker::internal::facade;
using broker::internal::native;
using std::cout;
using std::endl;
using std::string;

using namespace broker;
using namespace broker::detail;

namespace atom = broker::internal::atom;

namespace {

struct fixture : base_fixture {
  fixture() {
    auto x = caf::make_node_id(10, "402FA79E64ACFA54522FFC7AC886630670517900");
    if (!x)
      FAIL("caf::make_node_id failed");
    node = std::move(*x);
    node_str = to_string(node);
  }

  void push(error x) {
    data xs;
    if (!convert(x, xs))
      FAIL("unable to convert error to data");
    caf::anon_send(native(ep.core()), atom::publish_v, atom::local_v,
                   make_data_message(topic::errors(), std::move(xs)));
  }

  void push(status x) {
    data xs;
    if (!convert(x, xs))
      FAIL("unable to convert status to data");
    caf::anon_send(native(ep.core()), atom::publish_v, atom::local_v,
                   make_data_message(topic::statuses(), std::move(xs)));
  }

  caf::node_id node;

  std::string node_str;
};

} // namespace <anonymous>

CAF_TEST_FIXTURE_SCOPE(status_subscriber_tests, fixture)

CAF_TEST(base_tests) {
  auto sub1 = ep.make_status_subscriber(true);
  auto sub2 = ep.make_status_subscriber(false);
  for (auto sub : {&sub1, &sub2})
    sub->set_rate_calculation(false);
  run();
  CAF_REQUIRE_EQUAL(sub1.available(), 0u);
  CAF_REQUIRE_EQUAL(sub2.available(), 0u);
  CAF_MESSAGE("test error event");
  error e1 = ec::type_clash;
  push(e1);
  run();
  CAF_REQUIRE_EQUAL(sub1.available(), 1u);
  CAF_REQUIRE(sub1.get() == status_variant{e1});
  CAF_REQUIRE_EQUAL(sub2.available(), 1u);
  CAF_REQUIRE(sub2.get() == status_variant{e1});
  CAF_MESSAGE("test status event");
  auto s1 = status::make<sc::endpoint_discovered>(facade(node), "foobar");
  push(s1);
  run();
  CAF_REQUIRE_EQUAL(sub1.available(), 1u);
  CAF_REQUIRE(sub1.get() == status_variant{s1});
  CAF_REQUIRE_EQUAL(sub2.available(), 0u);
  CAF_MESSAGE("shutdown");
  anon_send_exit(native(ep.core()), caf::exit_reason::user_shutdown);
}

CAF_TEST_FIXTURE_SCOPE_END()
