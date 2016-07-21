#include "broker/broker.hh"

#include "broker/detail/memory_backend.hh"

#define SUITE backend
#include "test.hpp"

using namespace broker;

namespace {

struct fixture {
  fixture() : backend{&memory_backend} {
    // nop
  }

  detail::memory_backend memory_backend;
  detail::abstract_backend* backend;
};

} // namespace <anonymous>

FIXTURE_SCOPE(memory_backend_tests, fixture)

TEST(put/get) {
  auto put = backend->put("foo", 7);
  REQUIRE(put);
  auto get = backend->get("foo");
  REQUIRE(get);
  CHECK_EQUAL(*get, data{7});
  MESSAGE("overwrite");
  put = backend->put("foo", 42);
  REQUIRE(put);
  get = backend->get("foo");
  REQUIRE(get);
  CHECK_EQUAL(*get, data{42});
  MESSAGE("no key");
  get = backend->get("bar");
  REQUIRE(!get);
  CHECK_EQUAL(get.error(), ec::no_such_key);
}

TEST(add/remove) {
  auto add = backend->add("foo", 42);
  REQUIRE(!add);
  CHECK_EQUAL(add.error(), ec::no_such_key);
  auto remove = backend->remove("foo", 42);
  REQUIRE(!remove);
  CHECK_EQUAL(remove.error(), ec::no_such_key);
  auto put = backend->put("foo", 42);
  MESSAGE("add");
  add = backend->add("foo", 2);
  REQUIRE(add);
  auto get = backend->get("foo");
  REQUIRE(get);
  CHECK_EQUAL(*get, data{44});
  MESSAGE("remove");
  remove = backend->remove("foo", "bar");
  REQUIRE(!remove);
  CHECK_EQUAL(remove.error(), ec::type_clash);
  remove = backend->remove("foo", 10);
  REQUIRE(remove);
  get = backend->get("foo");
  REQUIRE(get);
  CHECK_EQUAL(*get, data{34});
}

TEST(erase) {
  auto erase = backend->erase("foo");
  REQUIRE(erase);
  CHECK(!*erase); // no such key
  auto put = backend->put("foo", "bar");
  REQUIRE(put);
  erase = backend->erase("foo");
  REQUIRE(erase);
  CHECK(*erase);
}

TEST(expiration) {
  using namespace std::chrono;
  auto put = backend->put("foo", "bar", time::now() + milliseconds(50));
  REQUIRE(put);
  std::this_thread::sleep_for(milliseconds(10));
  auto expire = backend->expire("foo");
  REQUIRE(expire);
  CHECK(!*expire); // too early
  auto exists = backend->exists("foo");
  REQUIRE(exists);
  CHECK(*exists);
  std::this_thread::sleep_for(milliseconds(40));
  expire = backend->expire("foo");
  REQUIRE(expire);
  CHECK(*expire); // success: time of call > expiry
  exists = backend->exists("foo");
  REQUIRE(exists);
  CHECK(!*exists); // element removed
}

FIXTURE_SCOPE_END()
