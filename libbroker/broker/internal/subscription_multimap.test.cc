#include "broker/internal/subscription_multimap.hh"

#include "broker/broker-test.test.hh"

#include <set>
#include <vector>

TEST(insert discards duplicates) {
  broker::internal::subscription_multimap<int> uut;
  CHECK(uut.insert("foo", 1));
  CHECK(!uut.insert("foo", 1));
}

TEST(purge_value erases all entries that contain the value) {
  broker::internal::subscription_multimap<int> uut;
  uut.insert("foo", 1);
  uut.insert("foo", 2);
  uut.insert("bar", 1);
  CHECK_EQUAL(uut.purge_value(1), 2);
  CHECK_EQUAL(uut.at("foo"), (std::set{2}));
  CHECK(!uut.contains("bar"));
}

TEST(select returns all values that match the topic) {
  broker::internal::subscription_multimap<int> uut;
  uut.insert("/foo/bar", 1);
  uut.insert("/foo/baz", 2);
  uut.insert("/qux", 3);
  uut.insert("/foo", 4);
  uut.insert("/foo/foo", 4);
  CHECK_EQUAL(uut.select("/foo/bar"), (std::vector{1, 4}));
  CHECK_EQUAL(uut.select("/foo/baz"), (std::vector{2, 4}));
  CHECK_EQUAL(uut.select("/foo"), (std::vector{4}));
  CHECK_EQUAL(uut.select("/qux"), (std::vector{3}));
  CHECK_EQUAL(uut.select("/"), (std::vector<int>{}));
  CHECK_EQUAL(uut.select("/foo/bar/baz"), (std::vector{1, 4}));
}
