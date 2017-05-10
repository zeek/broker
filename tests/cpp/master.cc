#define CAF_SUITE master
#include "test.hpp"
#include <caf/test/dsl.hpp>

#include "broker/broker.hh"
#include "broker/detail/aliases.hh"
#include "broker/detail/core_actor.hh"

using std::cout;
using std::endl;
using std::string;

using namespace caf;
using namespace broker;
using namespace broker::detail;

CAF_TEST_FIXTURE_SCOPE(store_master, test_coordinator_context_fixture)

CAF_TEST(local_master) {
  auto core = ctx.core();
  endpoint ep{ctx};
  sched.run();
  sched.inline_next_enqueue(); // ep.attach talks to the core (blocking)
  // ep.attach sends a message to the core that will then spawn a new master
  auto expected_ds = ep.attach<master, memory>("foo");
  CAF_REQUIRE(expected_ds.engaged());
  auto& ds = *expected_ds;
  auto ms = ds.frontend();
  // the core adds the master immediately to the topic and sends a stream
  // handshake
  expect((stream_msg::open),
         from(_).to(ms).with(_, core, _, _, _, _, false));
  expect((stream_msg::ack_open),
         from(ms).to(core).with(_, 5, _, false));
  // test putting something into the store
  ds.put("hello", "world");
  expect((put_command), from(_).to(ms).with(_));
  // read back what we have written
  sched.inline_next_enqueue(); // ds.get talks to the master_actor (blocking)
  auto res = ds.get("hello");
  CAF_CHECK_EQUAL(res, data{"world"});
  // check the name of the master
  sched.inline_next_enqueue(); // ds.name talks to the master_actor (blocking)
  auto n = ds.name();
  CAF_CHECK_EQUAL(n, "foo");
  // done
  anon_send_exit(core, exit_reason::user_shutdown);
  sched.run();
}

CAF_TEST_FIXTURE_SCOPE_END()
