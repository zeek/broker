#define SUITE master

#include "broker/store.hh"

#include "test.hh"

#include <chrono>
#include <regex>

#include "broker/backend.hh"
#include "broker/data.hh"
#include "broker/defaults.hh"
#include "broker/endpoint.hh"
#include "broker/error.hh"
#include "broker/filter_type.hh"
#include "broker/internal/clone_actor.hh"
#include "broker/internal/master_actor.hh"
#include "broker/internal/native.hh"
#include "broker/internal/type_id.hh"
#include "broker/internal_command.hh"
#include "broker/store_event.hh"
#include "broker/topic.hh"

using broker::internal::native;
using std::cout;
using std::endl;
using std::string;

namespace atom = broker::internal::atom;

using namespace broker;
using namespace broker::detail;

using namespace std::literals::chrono_literals;

namespace {

using string_list = std::vector<string>;

class pattern_list {
public:
  explicit pattern_list(std::initializer_list<const char*> strings) {
    patterns_.reserve(strings.size());
    for (auto str : strings)
      patterns_.emplace_back(str, str);
  }

  pattern_list(const pattern_list&) = default;

  auto begin() const {
    return patterns_.begin();
  }

  auto end() const {
    return patterns_.end();
  }

private:
  std::vector<std::pair<std::string, std::regex>> patterns_;
};

std::string to_string(const pattern_list& xs) {
  auto i = xs.begin();
  auto e = xs.end();
  if (i == e)
    return "[]";
  std::string result = "[";
  auto append_quoted = [&](const std::string& x) {
    result += '"';
    result += x;
    result += '"';
  };
  append_quoted(i->first);
  for (++i; i != e; ++i) {
    result += ", ";
    append_quoted(i->first);
  }
  result += ']';
  return result;
}

bool operator==(const string_list& xs, const pattern_list& ys) {
  auto matches = [](const std::string& x, const auto& y) {
    return std::regex_match(x, y.second);
  };
  return std::equal(xs.begin(), xs.end(), ys.begin(), ys.end(), matches);
}

struct fixture : base_fixture {
  string_list log;
  worker logger;

  caf::timespan tick_interval = defaults::store::tick_interval;

  fixture() {
    logger = ep.subscribe(
      // Topics.
      {topic::store_events()},
      // Init.
      [] {},
      // Consume.
      [this](data_message msg) {
        auto content = get_data(msg);
        if (auto insert = store_event::insert::make(content))
          log.emplace_back(to_string(insert));
        else if (auto update = store_event::update::make(content))
          log.emplace_back(to_string(update));
        else if (auto erase = store_event::erase::make(content))
          log.emplace_back(to_string(erase));
        else
          FAIL("unknown event: " << to_string(content));
      },
      // Cleanup.
      [](const error&) {});
  }

  ~fixture() {
    anon_send_exit(internal::native(logger), caf::exit_reason::user_shutdown);
  }
};

} // namespace

FIXTURE_SCOPE(local_store_master, fixture)

TEST(local_master) {
  auto core = native(ep.core());
  run(tick_interval);
  sched.inline_next_enqueue(); // ep.attach talks to the core (blocking)
  // ep.attach sends a message to the core that will then spawn a new master
  auto expected_ds = ep.attach_master("foo", backend::memory);
  REQUIRE(expected_ds.engaged());
  auto& ds = *expected_ds;
  MESSAGE(ds.frontend_id());
  auto ms = ds.frontend();
  // the core adds the master immediately to the topic and sends a stream
  // handshake
  run(tick_interval);
  // test putting something into the store
  ds.put("hello", "world");
  run(tick_interval);
  // read back what we have written
  sched.inline_next_enqueue(); // ds.get talks to the master_actor (blocking)
  CHECK_EQUAL(value_of(ds.get("hello")), data{"world"});
  // check the name of the master
  sched.inline_next_enqueue(); // ds.name talks to the master_actor (blocking)
  auto n = ds.name();
  CHECK_EQUAL(n, "foo");
  // override the existing value
  ds.put("hello", "universe");
  run(tick_interval);
  // read back what we have written
  sched.inline_next_enqueue(); // ds.get talks to the master_actor (blocking)
  CHECK_EQUAL(value_of(ds.get("hello")), data{"universe"});
  // cleanup
  ds.clear();
  run(tick_interval);
  sched.inline_next_enqueue();
  CHECK_EQUAL(error_of(ds.get("hello")), ec::no_such_key);
  // test put_unique
  sched.inline_next_enqueue(); // ds.put_unique also talks to the master_actor
  CHECK_EQUAL(unbox(ds.put_unique("bar", "baz")), data{true});
  sched.inline_next_enqueue(); // ds.put_unique also talks to the master_actor
  CHECK_EQUAL(unbox(ds.put_unique("bar", "unicorn")), data{false});
  // check log
  run(tick_interval);
  CHECK_EQUAL(log, pattern_list({
                     "insert\\(foo, hello, world, (null|none), .+\\)",
                     "update\\(foo, hello, world, universe, (null|none), .+\\)",
                     "erase\\(foo, hello, .+\\)",
                     "insert\\(foo, bar, baz, .+\\)",
                   }));
  // done
  caf::anon_send_exit(core, caf::exit_reason::user_shutdown);
}

FIXTURE_SCOPE_END()

/*
FIXTURE_SCOPE(store_master, net_fixture<fixture>)

TEST(master_with_clone) {
  caf::timespan tick_interval = defaults::store::tick_interval;
  // --- phase 1: get state from fixtures and initialize cores -----------------
  auto core1 = native(earth.ep.core());
  auto core2 = native(mars.ep.core());
  // --- phase 2: connect earth and mars at CAF level --------------------------
  // Prepare publish and remote_actor calls.
  MESSAGE("prepare connections on earth and mars");
  prepare_connection(mars, earth, "mars", 8080u);
  // Run any initialization code.
  run(tick_interval);
  // Tell mars to listen for peers.
  MESSAGE("publish core on mars");
  mars.sched.inline_next_enqueue(); // listen() calls middleman().publish()
  auto res = mars.ep.listen("", 8080u);
  CHECK_EQUAL(res, 8080u);
  run(tick_interval);
  // Establish connection between mars and earth before peering in order to
  // connect the streaming parts of CAF before we go into Broker code.
  MESSAGE("connect mars and earth");
  auto core2_proxy = earth.remote_actor("mars", 8080u);
  run(tick_interval);
  // --- phase 4: attach a master on earth -------------------------------------
  MESSAGE("attach a master on earth");
  earth.sched.inline_next_enqueue();
  auto expected_ds_earth = earth.ep.attach_master("foo", backend::memory);
  if (!expected_ds_earth)
    FAIL(
      "could not attach master: " << to_string(expected_ds_earth.error()));
  auto& ds_earth = *expected_ds_earth;
  auto ms_earth = native(ds_earth.frontend());
  // the core adds the master immediately to the topic and sends a stream
  // handshake
  run(tick_interval);
  // Store some test data in the master.
  expected_ds_earth->put("test", 123);
  expect_on(earth, (atom::local, internal_command), from(_).to(ms_earth));
  run(tick_interval);
  earth.sched.inline_next_enqueue(); // .get talks to the master
  CHECK_EQUAL(value_of(ds_earth.get("test")), data{123});
  // --- phase 5: peer from earth to mars --------------------------------------
  auto foo_master = "foo" / topic::master_suffix();
  // Initiate handshake between core1 and core2.
  earth.self->send(core1, atom::peer_v, core2_proxy.node(), core2_proxy);
  run(tick_interval);
  // --- phase 6: attach a clone on mars ---------------------------------------
  mars.sched.inline_next_enqueue();
  MESSAGE("attach a clone on mars");
  mars.sched.inline_next_enqueue();
  auto expected_ds_mars = mars.ep.attach_clone("foo");
  REQUIRE(expected_ds_mars.engaged());
  run(tick_interval);
  // -- phase 7: run it all & check results ------------------------------------
  auto& ds_mars = *expected_ds_mars;
  auto cl_mars = ds_mars.frontend();
  MESSAGE("put 'user' -> 'neverlord'");
  ds_mars.put("user", "neverlord");
  expect_on(mars, (atom::local, internal_command),
            from(_).to(native(ds_mars.frontend())));
  auto run_until_idle = [&] {
    auto idle = [&] {
      return earth.deref<detail::master_actor_type>(ms_earth).state.idle()
             && mars.deref<detail::clone_actor_type>(cl_mars).state.idle();
    };
    size_t iteration = 0;
    do {
      if (++iteration == 100)
        FAIL("system reached no idle state within 100 ticks");
      run(tick_interval);
    } while (!idle());
  };
  run_until_idle();
  MESSAGE("once clone and master are idle, they are in sync");
  earth.sched.inline_next_enqueue();
  CHECK_EQUAL(value_of(ds_earth.get("test")), data{123});
  earth.sched.inline_next_enqueue();
  CHECK_EQUAL(value_of(ds_earth.get("user")), data{"neverlord"});
  mars.sched.inline_next_enqueue();
  CHECK_EQUAL(value_of(ds_mars.get("test")), data{123});
  mars.sched.inline_next_enqueue();
  CHECK_EQUAL(value_of(ds_mars.get("user")), data{"neverlord"});
  MESSAGE("put_unique propagates the status back to the store object");
  mars.sched.after_next_enqueue(run_until_idle);
  CHECK_EQUAL(value_of(ds_mars.put_unique("bar", "baz")), data{true});
  mars.sched.after_next_enqueue(run_until_idle);
  CHECK_EQUAL(value_of(ds_mars.put_unique("bar", "unicorn")), data{false});
  // done
  anon_send_exit(native(earth.ep.core()), caf::exit_reason::user_shutdown);
  anon_send_exit(native(mars.ep.core()), caf::exit_reason::user_shutdown);
  exec_all();
  // check log
  CHECK_EQUAL(mars.log, earth.log);
  CHECK_EQUAL(mars.log, pattern_list({
                          "insert\\(foo, test, 123, (null|none), .+\\)",
                          "insert\\(foo, user, neverlord, (null|none), .+\\)",
                          "insert\\(foo, bar, baz, (null|none), .+\\)",
                        }));
}

FIXTURE_SCOPE_END()
*/
