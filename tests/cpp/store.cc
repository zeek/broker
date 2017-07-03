#include "broker/broker.hh"

#define SUITE store
#include "test.hpp"

using namespace broker;

TEST(default construction) {
  store{};
  store::proxy{};
}

TEST(backend option passing) {
  endpoint ep;
  auto opts = backend_options{{"foo", 4.2}};
  auto ds = ep.attach<master>("lord", memory, std::move(opts));
  REQUIRE(ds);
  ep.detach_all();
}

TEST(master operations) {
  endpoint ep;
  auto ds = ep.attach<master, memory>("kono");
  REQUIRE(ds);
  MESSAGE("put");
  ds->put("foo", 42);
  REQUIRE_EQUAL(ds->get("foo"), data{42});
  REQUIRE_EQUAL(ds->get("bar"), error{ec::no_such_key});
  MESSAGE("erase");
  ds->erase("foo");
  REQUIRE_EQUAL(ds->get("foo"), error{ec::no_such_key});
  MESSAGE("add");
  ds->add("foo", 1u); // key did not exist, operation fails
  REQUIRE(!ds->get("foo"));
  ds->put("foo", 0u);
  ds->add("foo", 1u); // key exists now, operation succeeds
  REQUIRE_EQUAL(ds->get("foo"), data{1u});
  ds->add("foo", 41u); // adding on top of existing value
  REQUIRE_EQUAL(ds->get("foo"), data{42u});
  ds->put("foo", "b");
  ds->add("foo", "a");
  ds->add("foo", "r");
  REQUIRE_EQUAL(ds->get("foo"), data{"bar"});
  ds->put("foo", set{1, 3});
  ds->add("foo", 2);
  REQUIRE_EQUAL(ds->get("foo"), data(set{1, 2, 3}));
  MESSAGE("subtract");
  ds->subtract("foo", 1);
  REQUIRE_EQUAL(ds->get("foo"), data(set{2, 3}));
  MESSAGE("get overload");
  REQUIRE_EQUAL(ds->get("foo", 1), data{false});
  REQUIRE_EQUAL(ds->get("foo", 2), data{true});
  MESSAGE("keys");
  REQUIRE_EQUAL(ds->keys(), data(set{"foo"}));
  ep.detach_all();
}

TEST(clone operations - same endpoint) {
  endpoint ep;
  auto m = ep.attach<master, memory>("vulcan");
  MESSAGE("master PUT");
  m->put("key", "value");
  REQUIRE(m);
  auto c = ep.attach<broker::clone>("vulcan");
  REQUIRE(!c);
  ep.detach_all();
}

TEST(expiration) {
  using std::chrono::milliseconds;
  endpoint ep;
  auto m = ep.attach<master, memory>("grubby");
  REQUIRE(m);
  auto expiry = milliseconds(100);
  m->put("foo", 42, expiry);
  // Check within validity interval.
  std::this_thread::sleep_for(milliseconds(50));
  auto v = m->get("foo");
  REQUIRE(v);
  CHECK_EQUAL(v, data{42});
  std::this_thread::sleep_for(milliseconds(50));
  // Check after expiration.
  v = m->get("foo");
  REQUIRE(!v);
  CHECK(v.error() == ec::no_such_key);
  ep.detach_all();
}

TEST(proxy) {
  endpoint ep;
  auto m = ep.attach<master, memory>("puneta");
  REQUIRE(m);
  m->put("foo", 42);
  MESSAGE("master: issue queries");
  auto proxy = store::proxy{*m};
  auto id = proxy.get("foo");
  CHECK_EQUAL(id, 1u);
  id = proxy.get("bar");
  CHECK_EQUAL(id, 2u);
  MESSAGE("master: collect responses");
  auto resp = proxy.receive();
  CHECK_EQUAL(resp.id, 1u);
  REQUIRE_EQUAL(resp.answer, data{42});
  resp = proxy.receive();
  CHECK_EQUAL(resp.id, 2u);
  REQUIRE_EQUAL(resp.answer, error{ec::no_such_key});
  auto key_id = proxy.keys();
  auto key_resp = proxy.receive();
  CAF_REQUIRE_EQUAL(key_resp.id, key_id);
  CAF_REQUIRE_EQUAL(key_resp.answer, data(set{"foo"}));
  ep.detach_all();
}
