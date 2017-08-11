#define SUITE master

#include "test.hpp"
#include <caf/test/io_dsl.hpp>

#include "broker/broker.hh"

#include "broker/detail/core_actor.hh"
#include "broker/detail/filter_type.hh"

using std::cout;
using std::endl;
using std::string;

using namespace caf;
using namespace broker;
using namespace broker::detail;

CAF_TEST_FIXTURE_SCOPE(local_store_master, base_fixture)

CAF_TEST(local_master) {
  auto core = ep.core();
  sched.run();
  sched.inline_next_enqueue(); // ep.attach talks to the core (blocking)
  // ep.attach sends a message to the core that will then spawn a new master
  auto expected_ds = ep.attach_master("foo", memory);
  CAF_REQUIRE(expected_ds.engaged());
  auto& ds = *expected_ds;
  auto ms = ds.frontend();
  // the core adds the master immediately to the topic and sends a stream
  // handshake
  sched.run();
  // test putting something into the store
  ds.put("hello", "world");
  sched.run();
  // read back what we have written
  sched.inline_next_enqueue(); // ds.get talks to the master_actor (blocking)
  CAF_CHECK_EQUAL(ds.get("hello"), data{"world"});
  // check the name of the master
  sched.inline_next_enqueue(); // ds.name talks to the master_actor (blocking)
  auto n = ds.name();
  CAF_CHECK_EQUAL(n, "foo");
  // send put command to the master's topic
  anon_send(core, atom::publish::value, atom::local::value,
            n / topics::reserved / topics::master,
            make_internal_command<put_command>("hello", "universe"));
  sched.run();
  // read back what we have written
  sched.inline_next_enqueue(); // ds.get talks to the master_actor (blocking)
  CAF_CHECK_EQUAL(ds.get("hello"), data{"universe"});
  ds.clear();
  sched.run();
  sched.inline_next_enqueue();
  CAF_CHECK_EQUAL(ds.get("hello"), caf::error{ec::no_such_key});
  // done
  anon_send_exit(core, exit_reason::user_shutdown);
  sched.run();
  sched.inline_all_enqueues();
}

CAF_TEST_FIXTURE_SCOPE_END()

CAF_TEST_FIXTURE_SCOPE(store_master,
                       point_to_point_fixture<fake_network_fixture>)

CAF_TEST(master_with_clone) {
  // --- phase 1: get state from fixtures and initialize cores -----------------
  auto core1 = earth.ep.core();
  auto core2 = mars.ep.core();
  auto forward_stream_traffic = [&] {
    while (earth.mpx.try_exec_runnable() || mars.mpx.try_exec_runnable()
           || earth.mpx.read_data() || mars.mpx.read_data()) {
      // rince and repeat
    }
  };
  anon_send(core1, atom::no_events::value);
  anon_send(core2, atom::no_events::value);
  // --- phase 2: connect earth and mars at CAF level --------------------------
  // Prepare publish and remote_actor calls.
  CAF_MESSAGE("prepare connections on earth and mars");
  prepare_connection(mars, earth, "mars", 8080u);
  // Run any initialization code.
  exec_all();
  // Tell mars to listen for peers.
  CAF_MESSAGE("publish core on mars");
  mars.sched.inline_next_enqueue(); // listen() calls middleman().publish()
  auto res = mars.ep.listen("", 8080u);
  CAF_CHECK_EQUAL(res, 8080u);
  exec_all();
  // Establish connection between mars and earth before peering in order to
  // connect the streaming parts of CAF before we go into Broker code.
  CAF_MESSAGE("connect mars and earth");
  auto core2_proxy = earth.remote_actor("mars", 8080u);
  exec_all();
  // --- phase 4: attach a master on earth -------------------------------------
  CAF_MESSAGE("attach a master on earth");
  earth.sched.inline_next_enqueue();
  auto expected_ds_earth = earth.ep.attach_master("foo", memory);
  if (!expected_ds_earth)
    CAF_FAIL("could not attach master: "
             << earth.sys.render(expected_ds_earth.error()));
  auto& ds_earth = *expected_ds_earth;
  auto ms_earth = ds_earth.frontend();
  // the core adds the master immediately to the topic and sends a stream
  // handshake
  expect_on(earth, (stream_msg::open),
            from(_).to(ms_earth).with(_, core1, _, _, _, _, false));
  exec_all(); // skip remaining handshake
  // Store some test data in the master.
  expected_ds_earth->put("test", 123);
  expect_on(earth , (atom_value, internal_command),
            from(_).to(ms_earth).with(_, _));
  exec_all();
  earth.sched.inline_next_enqueue(); // .get talks to the master
  CAF_CHECK_EQUAL(ds_earth.get("test"), data{123});
  // --- phase 5: peer from earth to mars --------------------------------------
  auto foo_master = "foo" / topics::reserved / topics::master;
  // Initiate handshake between core1 and core2.
  earth.self->send(core1, atom::peer::value, core2_proxy);
  expect_on(earth, (atom::peer, actor),
            from(earth.self).to(core1).with(_, core2_proxy));
  // Step #1: core1  --->    ('peer', filter_type)    ---> core2
  forward_stream_traffic();
  expect_on(mars, (atom::peer, filter_type, actor),
            from(_).to(core2).with(_, filter_type{foo_master}, _));
  // Step #2: core1  <---   (stream_msg::open)   <--- core2
  forward_stream_traffic();
  expect_on(earth, (stream_msg::open),
            from(_).to(core1).with(
              std::make_tuple(_, filter_type{}, core2_proxy), _, _, _, false));
  // Step #3: core1  --->   (stream_msg::open)   ---> core2
  //          core1  ---> (stream_msg::ack_open) ---> core2
  forward_stream_traffic();
  expect_on(mars, (stream_msg::open),
            from(_).to(core2).with(_, _, _, _, false));
  expect_on(mars, (stream_msg::ack_open),
            from(_).to(core2).with(_, _, _, _, false));
  // Step #4: core1  <--- (stream_msg::ack_open) <--- core2
  forward_stream_traffic();
  expect_on(earth, (stream_msg::ack_open),
            from(_).to(core1).with(_, _, _, _, false));
  // Make sure there is no communication pending at this point.
  exec_all();
  // --- phase 7: resolve master for foo proactively ---------------------------
  CAF_MESSAGE("resolve master on mars");
  anon_send(core2, atom::store::value, atom::master::value,
            atom::resolve::value, "foo");
  expect_on(mars, (atom_value, atom_value, atom_value, std::string),
            from(_).to(core2).with(_, _, _, "foo"));
  mars.sched.run_once(); // run resolver
  network_traffic();
  expect_on(earth, (atom::store, atom::master, atom::get, std::string),
            from(_).to(core1).with(_, _, _, "foo"));
  network_traffic();
  mars.sched.run_once(); // run resolver
  expect_on(mars, (actor), from(_).to(core2).with(_));
  // --- phase 7: attach a clone on mars ---------------------------------------
  mars.sched.inline_next_enqueue();
  CAF_MESSAGE("attach a clone on mars");
  mars.sched.inline_next_enqueue();
  auto expected_ds_mars = mars.ep.attach_clone("foo");
  CAF_REQUIRE(expected_ds_mars.engaged());
  auto& ds_mars = *expected_ds_mars;
  auto& ms_mars = ds_mars.frontend();
  // the core adds the clone immediately to the topic and sends a stream
  // handshake
  auto foo_clone = "foo" / topics::reserved / topics::clone;
  expect_on(mars, (stream_msg::open),
            from(_).to(ms_mars).with(_, core2, _, _, _, _, false));
  expect_on(mars, (stream_msg::ack_open),
            from(ms_mars).to(core2).with(_, _, _, false));
  // the core also updates its filter on all peers ...
  network_traffic();
  expect_on(earth, (atom::update, filter_type),
            from(_).to(core1).with(_, filter_type{foo_clone}));
  // ... and requests the master to generate a new snapshot
  forward_stream_traffic();
  expect_on(earth, (stream_msg::batch), from(_).to(core1).with(1, _, 0));
  // -- phase 8: run it all & check results ------------------------------------
  exec_all();
  CAF_MESSAGE("put 'user' -> 'neverlord'");
  ds_mars.put("user", "neverlord");
  expect_on(mars, (atom_value, internal_command),
            from(_).to(ds_mars.frontend()).with(atom::local::value, _));
  expect_on(mars, (atom_value, topic, internal_command),
            from(_).to(mars.ep.core()).with(atom::publish::value, _, _));
  exec_all();
  earth.sched.inline_next_enqueue(); // .get talks to the master
  CAF_CHECK_EQUAL(ds_earth.get("user"), data{"neverlord"});
  mars.sched.inline_next_enqueue(); // .get talks to the master
  CAF_CHECK_EQUAL(ds_mars.get("test"), data{123});
  mars.sched.inline_next_enqueue(); // .get talks to the master
  CAF_CHECK_EQUAL(ds_mars.get("user"), data{"neverlord"});
  // done
  anon_send_exit(earth.ep.core(), exit_reason::user_shutdown);
  anon_send_exit(mars.ep.core(), exit_reason::user_shutdown);
  exec_all();
  mars.sched.inline_all_enqueues();
  earth.sched.inline_all_enqueues();
}

CAF_TEST_FIXTURE_SCOPE_END()
