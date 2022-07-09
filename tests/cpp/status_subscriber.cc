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

// Note: publishing on topic::errors or topic::statuses is of course EVIL and
//       should *never* happen in production code.
struct fixture : base_fixture {
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
};

} // namespace

FIXTURE_SCOPE(status_subscriber_tests, fixture)

TEST(base_tests) {
  auto sub1 = ep.make_status_subscriber(true);
  auto sub2 = ep.make_status_subscriber(false);
  run();
  REQUIRE_EQUAL(sub1.available(), 0u);
  REQUIRE_EQUAL(sub2.available(), 0u);
  MESSAGE("test error event");
  error e1 = ec::type_clash;
  push(e1);
  run();
  REQUIRE_EQUAL(sub1.available(), 1u);
  REQUIRE(sub1.get() == status_variant{e1});
  REQUIRE_EQUAL(sub2.available(), 1u);
  REQUIRE(sub2.get() == status_variant{e1});
  MESSAGE("test status event");
  auto s1 = status::make<sc::endpoint_discovered>(ids['B'], "foobar");
  push(s1);
  run();
  REQUIRE_EQUAL(sub1.available(), 1u);
  REQUIRE(sub1.get() == status_variant{s1});
  REQUIRE_EQUAL(sub2.available(), 0u);
}

FIXTURE_SCOPE_END()
