#include "broker/broker.hh"

#define SUITE store
#include "test.hpp"

using namespace broker;

namespace {

const auto propagation_delay = std::chrono::milliseconds(200);

} // namespace <anonymous>

TEST(no duplicate masters) {
  context ctx;
  auto ep0 = ctx.spawn<blocking>();
  auto ep1 = ctx.spawn<blocking>();
  ep0.peer(ep1);
  auto ds0 = ep0.attach<store::master>("yoda");
  REQUIRE(ds0);
  CHECK_EQUAL(ds0->name(), "yoda");
  std::this_thread::sleep_for(propagation_delay); // subscription
  auto ds1 = ep1.attach<store::master>("yoda");
  CHECK(ds1 == ec::master_exists);
}

TEST(master operations) {
  context ctx;
  auto ep = ctx.spawn<blocking>();
  auto ds = ep.attach<store::master>("kono");
  REQUIRE(ds);
  MESSAGE("put");
  ds->put("foo", 42);
  auto result = ds->get("foo");
  REQUIRE(result);
  CHECK_EQUAL(*result, data{42});
  result = ds->get("bar");
  REQUIRE(!result);
  CHECK_EQUAL(result.error(), ec::no_such_key);
  MESSAGE("erase");
  ds->erase("foo");
  result = ds->get("foo");
  REQUIRE(!result);
  CHECK_EQUAL(result.error(), ec::no_such_key);
  MESSAGE("add");
  ds->add("foo", 1u); // key did not exist, operation succeeds
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

TEST(clone operations - same endpoint) {
  context ctx;
  auto ep = ctx.spawn<blocking>();
  auto master = ep.attach<store::master>("vulcan");
  MESSAGE("master PUT");
  master->put("key", "value");
  REQUIRE(master);
  auto clone = ep.attach<store::clone>("vulcan");
  REQUIRE(clone);
  std::this_thread::sleep_for(propagation_delay); // snapshot transfer
  auto v = clone->get("key");
  REQUIRE(v);
  CHECK_EQUAL(v, data{"value"});
  MESSAGE("clone PUT");
  clone->put("key", 4.2);
  std::this_thread::sleep_for(propagation_delay);
  v = clone->get("key");
  REQUIRE(v);
  CHECK_EQUAL(v, data{4.2});
}

TEST(clone operations - different endpoints) {
  context ctx;
  auto ep0 = ctx.spawn<blocking>();
  auto ep1 = ctx.spawn<blocking>();
  ep0.peer(ep1);
  auto master = ep0.attach<store::master>("flaka");
  auto clone = ep1.attach<store::clone>("flaka");
  REQUIRE(master);
  REQUIRE(clone);
  clone->put("foo", 4.2);
  std::this_thread::sleep_for(propagation_delay); // master -> clone
  auto v = clone->get("foo");
  REQUIRE(v);
  CHECK_EQUAL(v, data{4.2});
  clone->remove("foo", 0.2);
  std::this_thread::sleep_for(propagation_delay); // master -> clone
  v = clone->get("foo");
  REQUIRE(v);
  CHECK_EQUAL(v, data{4.0});
}
