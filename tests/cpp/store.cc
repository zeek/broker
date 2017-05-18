#include "broker/broker.hh"

#define SUITE store
#include "test.hpp"

using namespace broker;

namespace {

const auto propagation_delay = std::chrono::milliseconds(100);

} // namespace <anonymous>

TEST(default construction) {
  store{};
  store::proxy{};
}

TEST(backend option passing) {
  endpoint ep;
  auto opts = backend_options{{"foo", 4.2}};
  auto ds = ep.attach<master>("lord", memory, std::move(opts));
  REQUIRE(ds);
}

/*
TEST(no duplicate masters) {
  context ctx;
  auto ep0 = ctx.spawn();
  auto ep1 = ctx.spawn();
  ep0.peer(ep1);
  auto ds0 = ep0.attach<master, memory>("yoda");
  REQUIRE(ds0);
  CHECK_EQUAL(ds0->name(), "yoda");
  std::this_thread::sleep_for(propagation_delay); // subscription
  auto ds1 = ep1.attach<master, memory>("yoda");
  CHECK(ds1 == ec::master_exists);
}
*/

TEST(master operations) {
  endpoint ep;
  auto ds = ep.attach<master, memory>("kono");
  REQUIRE(ds);
  MESSAGE("put");
  ds->put("foo", 42);
  auto x = ds->get("foo");
  REQUIRE(x);
  CHECK_EQUAL(*x, data{42});
  x = ds->get("bar");
  REQUIRE(!x);
  CHECK_EQUAL(x.error(), ec::no_such_key);
  MESSAGE("erase");
  ds->erase("foo");
  x = ds->get("foo");
  REQUIRE(!x);
  CHECK_EQUAL(x.error(), ec::no_such_key);
  MESSAGE("add");
  ds->add("foo", 1u); // key did not exist, operation fails
  x = ds->get("foo");
  REQUIRE(!x);
  ds->put("foo", 0u);
  ds->add("foo", 1u); // key exists now, operation succeeds
  x = ds->get("foo");
  REQUIRE(x);
  CHECK_EQUAL(*x, data{1u});
  ds->add("foo", 41u); // adding on top of existing value
  x = ds->get("foo");
  REQUIRE(x);
  CHECK_EQUAL(*x, data{42u});
  ds->put("foo", "b");
  ds->add("foo", "a");
  ds->add("foo", "r");
  x = ds->get("foo");
  REQUIRE(x);
  CHECK_EQUAL(*x, data{"bar"});
  ds->put("foo", set{1, 3});
  ds->add("foo", 2);
  x = ds->get("foo");
  REQUIRE(x);
  CHECK(*x == set{1, 2, 3});
  MESSAGE("subtract");
  ds->subtract("foo", 1);
  x = ds->get("foo");
  REQUIRE(x);
  CHECK(*x == set{2, 3});
  MESSAGE("get overload");
  x = ds->get("foo", 1);
  REQUIRE(x);
  CHECK_EQUAL(*x, data{false});
  x = ds->get("foo", 2);
  REQUIRE(x);
  CHECK_EQUAL(*x, data{true});
}

TEST(clone operations - same endpoint) {
  endpoint ep;
  auto m = ep.attach<master, memory>("vulcan");
  MESSAGE("master PUT");
  m->put("key", "value");
  REQUIRE(m);
  auto c = ep.attach<broker::clone>("vulcan");
  REQUIRE(c);
  std::this_thread::sleep_for(propagation_delay); // snapshot transfer
  auto v = c->get("key");
  REQUIRE(v);
  CHECK_EQUAL(v, data{"value"});
  MESSAGE("clone PUT");
  c->put("key", 4.2);
  std::this_thread::sleep_for(propagation_delay);
  v = c->get("key");
  REQUIRE(v);
  CHECK_EQUAL(v, data{4.2});
}

/*
TEST(clone operations - different endpoints) {
  context ctx;
  auto ep0 = ctx.spawn();
  auto ep1 = ctx.spawn();
  ep0.peer(ep1);
  auto m = ep0.attach<master, memory>("flaka");
  auto c = ep1.attach<broker::clone>("flaka");
  REQUIRE(m);
  REQUIRE(c);
  c->put("foo", 4.2);
  std::this_thread::sleep_for(propagation_delay); // master -> clone
  auto v = c->get("foo");
  REQUIRE(v);
  CHECK_EQUAL(v, data{4.2});
  c->subtract("foo", 0.2);
  std::this_thread::sleep_for(propagation_delay); // master -> clone
  v = c->get("foo");
  REQUIRE(v);
  CHECK_EQUAL(v, data{4.0});
}
*/

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
  REQUIRE(resp.answer);
  CHECK_EQUAL(*resp.answer, data{42});
  resp = proxy.receive();
  CHECK_EQUAL(resp.id, 2u);
  REQUIRE(!resp.answer);
  CHECK_EQUAL(resp.answer.error(), ec::no_such_key);
  MESSAGE("clone: issue queries");
  auto c = ep.attach<broker::clone>("puneta");
  REQUIRE(c);
  proxy = store::proxy{*c};
  id = proxy.get("foo");
  CHECK_EQUAL(id, 1u);
  id = proxy.get("bar");
  CHECK_EQUAL(id, 2u);
  MESSAGE("clone: collect responses");
  resp = proxy.receive();
  CHECK_EQUAL(resp.id, 1u);
  REQUIRE(resp.answer);
  CHECK_EQUAL(*resp.answer, data{42});
  resp = proxy.receive();
  CHECK_EQUAL(resp.id, 2u);
  REQUIRE(!resp.answer);
  CHECK_EQUAL(resp.answer.error(), ec::no_such_key);
}
