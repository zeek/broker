#include "broker/broker.hh"

#define SUITE store
#include "test.hpp"

using namespace broker;

namespace {

const auto propagation_delay = std::chrono::milliseconds(100);

} // namespace <anonymous>

TEST(backend option passing) {
  context ctx;
  auto ep = ctx.spawn<blocking>();
  auto opts = backend_options{{"foo", 4.2}};
  auto ds = ep.attach<master, memory>("lord", std::move(opts));
  REQUIRE(ds);
}

TEST(no duplicate masters) {
  context ctx;
  auto ep0 = ctx.spawn<blocking>();
  auto ep1 = ctx.spawn<blocking>();
  ep0.peer(ep1);
  auto ds0 = ep0.attach<master, memory>("yoda");
  REQUIRE(ds0);
  CHECK_EQUAL(ds0->name(), "yoda");
  std::this_thread::sleep_for(propagation_delay); // subscription
  auto ds1 = ep1.attach<master, memory>("yoda");
  CHECK(ds1 == sc::master_exists);
}

TEST(master operations) {
  context ctx;
  auto ep = ctx.spawn<blocking>();
  auto ds = ep.attach<master, memory>("kono");
  REQUIRE(ds);
  MESSAGE("put");
  ds->put("foo", 42);
  auto result = ds->get("foo");
  REQUIRE(result);
  CHECK_EQUAL(*result, data{42});
  result = ds->get("bar");
  REQUIRE(!result);
  CHECK_EQUAL(result.error(), sc::no_such_key);
  MESSAGE("erase");
  ds->erase("foo");
  result = ds->get("foo");
  REQUIRE(!result);
  CHECK_EQUAL(result.error(), sc::no_such_key);
  MESSAGE("add");
  ds->add("foo", 1u); // key did not exist, operation fails
  result = ds->get("foo");
  REQUIRE(!result);
  ds->put("foo", 0u);
  ds->add("foo", 1u); // key exists now, operation succeeds
  result = ds->get("foo");
  REQUIRE(result);
  CHECK_EQUAL(*result, data{1u});
  ds->add("foo", 41u); // adding on top of existing value
  result = ds->get("foo");
  REQUIRE(result);
  CHECK_EQUAL(*result, data{42u});
  ds->put("foo", "b");
  ds->add("foo", "a");
  ds->add("foo", "r");
  result = ds->get("foo");
  REQUIRE(result);
  CHECK_EQUAL(*result, data{"bar"});
  ds->put("foo", set{1, 3});
  ds->add("foo", 2);
  result = ds->get("foo");
  REQUIRE(result);
  CHECK(*result == set{1, 2, 3});
  MESSAGE("remove");
  ds->remove("foo", 1);
  result = ds->get("foo");
  REQUIRE(result);
  CHECK(*result == set{2, 3});
  MESSAGE("lookup");
  result = ds->lookup("foo", 1);
  REQUIRE(result);
  CHECK_EQUAL(*result, data{false});
  result = ds->lookup("foo", 2);
  REQUIRE(result);
  CHECK_EQUAL(*result, data{true});
}

TEST(nonblocking api) {
  context ctx;
  auto ep = ctx.spawn<blocking>();
  auto ds = ep.attach<master, memory>("captain");
  REQUIRE(ds);
  MESSAGE("put");
  ds->put("foo", 42);
  MESSAGE("existing key");
  auto result = std::make_shared<data>();
  ds->get<nonblocking>("foo").then(
    [=](data& d) { *result = std::move(d); },
    [](status) { /* nop */ }
  );
  std::this_thread::sleep_for(propagation_delay);
  REQUIRE(result);
  CHECK_EQUAL(*result, data{42});
  MESSAGE("non-existing key");
  auto failure = std::make_shared<status>();
  ds->get<nonblocking>("bar").then(
    [](const data&) { /* nop */ },
    [=](status s) { *failure = std::move(s); }
  );
  std::this_thread::sleep_for(propagation_delay);
  REQUIRE(failure);
  CHECK_EQUAL(*failure, sc::no_such_key);
}

TEST(clone operations - same endpoint) {
  context ctx;
  auto ep = ctx.spawn<blocking>();
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

TEST(clone operations - different endpoints) {
  context ctx;
  auto ep0 = ctx.spawn<blocking>();
  auto ep1 = ctx.spawn<blocking>();
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
  c->decrement("foo", 0.2);
  std::this_thread::sleep_for(propagation_delay); // master -> clone
  v = c->get("foo");
  REQUIRE(v);
  CHECK_EQUAL(v, data{4.0});
}

TEST(expiration) {
  using std::chrono::milliseconds;
  context ctx;
  auto ep = ctx.spawn<blocking>();
  auto m = ep.attach<master, memory>("grubby");
  REQUIRE(m);
  auto expiry = now() + milliseconds(100);
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
  CHECK(v.error() == sc::no_such_key);
}

TEST(proxy) {
  context ctx;
  auto ep = ctx.spawn<blocking>();
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
  CHECK_EQUAL(resp.id(), 1u);
  REQUIRE(resp);
  CHECK_EQUAL(*resp, data{42});
  resp = proxy.receive();
  CHECK_EQUAL(resp.id(), 2u);
  REQUIRE(!resp);
  CHECK_EQUAL(resp.status(), sc::no_such_key);
  MESSAGE("clone: issue queries");
  auto c = ep.attach<clone>("puneta");
  REQUIRE(c);
  proxy = store::proxy{*c};
  id = proxy.get("foo");
  CHECK_EQUAL(id, 1u);
  id = proxy.get("bar");
  CHECK_EQUAL(id, 2u);
  MESSAGE("clone: collect responses");
  resp = proxy.receive();
  CHECK_EQUAL(resp.id(), 1u);
  REQUIRE(resp);
  CHECK_EQUAL(*resp, data{42});
  resp = proxy.receive();
  CHECK_EQUAL(resp.id(), 2u);
  REQUIRE(!resp);
  CHECK_EQUAL(resp.status(), sc::no_such_key);
}
