#include "broker/broker.hh"

#include "broker/detail/filesystem.hh"
#include "broker/detail/make_unique.hh"
#include "broker/detail/memory_backend.hh"
#include "broker/detail/sqlite_backend.hh"

#define SUITE backend
#include "test.hpp"

using namespace broker;

namespace {

template <class T>
bool all_equal(const std::vector<T>& xs) {
  auto i = std::adjacent_find(xs.begin(), xs.end(), std::not_equal_to<T>{});
  return i == xs.end();
}

class meta_backend : public detail::abstract_backend {
  static constexpr auto filename = "/tmp/broker-unit-test-backend.sqlite";

public:
  bool initialize() {
    backends_.push_back(std::make_unique<detail::memory_backend>());
    auto sqlite = std::make_unique<detail::sqlite_backend>();
    detail::remove(filename);
    if (!sqlite->open(filename))
      return false;
    backends_.push_back(std::move(sqlite));
    return true;
  }

  expected<void> put(const data& key, data value,
                     optional<time::point> expiry) override {
    return perform<void>(
      [&](detail::abstract_backend& backend) {
        return backend.put(key, value, expiry);
      }
    );
  }

  expected<void> add(const data& key, const data& value,
                     optional<time::point> expiry) override {
    return perform<void>(
      [&](detail::abstract_backend& backend) {
        return backend.add(key, value, expiry);
      }
    );
  }

  expected<void> remove(const data& key, const data& value,
                        optional<time::point> expiry) override {
    return perform<void>(
      [&](detail::abstract_backend& backend) {
        return backend.remove(key, value, expiry);
      }
    );
  }

  expected<bool> erase(const data& key) override {
    return perform<bool>(
      [&](detail::abstract_backend& backend) {
        return backend.erase(key);
      }
    );
  }

  expected<bool> expire(const data& key) override {
    return perform<bool>(
      [&](detail::abstract_backend& backend) {
        return backend.expire(key);
      }
    );
  }

  expected<data> get(const data& key) const override {
    return perform<data>(
      [&](detail::abstract_backend& backend) {
        return backend.get(key);
      }
    );
  }

  expected<data> get(const data& key, const data& value) const override {
    return perform<data>(
      [&](detail::abstract_backend& backend) {
        return backend.get(key, value);
      }
    );
  }

  expected<bool> exists(const data& key) const override {
    return perform<bool>(
      [&](detail::abstract_backend& backend) {
        return backend.exists(key);
      }
    );
  }

  expected<uint64_t> size() const override {
    return perform<uint64_t>(
      [](detail::abstract_backend& backend) {
        return backend.size();
      }
    );
  }

  expected<broker::snapshot> snapshot() const override {
    return perform<broker::snapshot>(
      [](detail::abstract_backend& backend) {
        return backend.snapshot();
      }
    );
  }

private:
  template <class T, class F>
  expected<T> perform(F f) {
    std::vector<expected<T>> xs;
    for (auto& backend : backends_)
      xs.push_back(f(*backend));
    if (!all_equal(xs))
      return ec::unspecified;
    return std::move(xs.front());
  }

  template <class T, class F>
  expected<T> perform(F f) const {
    return const_cast<meta_backend*>(this)->perform<T>(f); // lazy
  }

  std::vector<std::unique_ptr<detail::abstract_backend>> backends_;
};

struct fixture {
  fixture() : backend{&meta_backend} {
    REQUIRE(meta_backend.initialize());
  }

  meta_backend meta_backend;
  detail::abstract_backend* backend;
};

} // namespace <anonymous>

FIXTURE_SCOPE(backend_tests, fixture)

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

TEST(erase/exists) {
  auto exists = backend->exists("foo");
  REQUIRE(exists);
  CHECK(!*exists);
  auto erase = backend->erase("foo");
  REQUIRE(erase);
  CHECK(!*erase); // no such key
  auto put = backend->put("foo", "bar");
  REQUIRE(put);
  exists = backend->exists("foo");
  REQUIRE(exists);
  CHECK(*exists);
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

TEST(snapshot) {
  auto put = backend->put("foo", "bar");
  REQUIRE(put);
  put = backend->put("bar", 4.2);
  REQUIRE(put);
  put = backend->put("baz", table{{"foo", true}, {"bar", false}});
  REQUIRE(put);
  auto size = backend->size();
  REQUIRE(size);
  CHECK_EQUAL(*size, 3u);
  auto ss = backend->snapshot();
  REQUIRE(ss);
  CHECK_EQUAL(ss->entries.size(), *size);
  CHECK_EQUAL(ss->entries.count("foo"), 1u);
}

FIXTURE_SCOPE_END()
