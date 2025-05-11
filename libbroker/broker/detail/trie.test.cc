#include "broker/detail/trie.hh"

#include "broker/broker-test.test.hh"

using namespace broker;
using namespace std::literals;

TEST(exact match) {
  detail::trie uut;
  uut.insert("this:key:has:a:long:prefix:3");
  uut.insert("this:key:has:a:long:common:prefix:2");
  uut.insert("this:key:has:a:long:common:prefix:1");
  CHECK(uut.contains("this:key:has:a:long:prefix:3"));
  CHECK(uut.contains("this:key:has:a:long:common:prefix:2"));
  CHECK(uut.contains("this:key:has:a:long:common:prefix:1"));
}

TEST(prefix match) {
  detail::trie uut;
  auto key1 = "this:key:has:a:long:prefix:3"sv;
  auto key2 = "this:key:has:a:long:common:prefix:2"sv;
  auto key3 = "this:key:has:a:long:common:prefix:1"sv;
  uut.insert(key1);
  uut.insert(key2);
  uut.insert(key3);
  for (auto i = 1; i < key1.size(); ++i) {
    if (!CHECK(uut.has_prefix(key1.substr(0, i)))) {
      std::cout << "Failed prefix lookup: " << key1.substr(0, i) << std::endl;
    }
  }
  for (auto i = 1; i < key2.size(); ++i) {
    CHECK(uut.has_prefix(key2.substr(0, i)));
  }
  for (auto i = 1; i < key3.size(); ++i) {
    CHECK(uut.has_prefix(key3.substr(0, i)));
  }
}

TEST(erase elements) {
  detail::trie uut;

  MESSAGE("inserting elements");
  uut.insert("this:key:has:a:long:prefix:3");
  uut.insert("this:key:has:a:long:prefix:3");
  uut.insert("this:key:has:a:long:common:prefix:2");
  uut.insert("this:key:has:a:long:common:prefix:1");

  MESSAGE("check that elements are present");
  CHECK(uut.contains("this:key:has:a:long:prefix:3"));
  CHECK(uut.contains("this:key:has:a:long:common:prefix:2"));
  CHECK(uut.contains("this:key:has:a:long:common:prefix:1"));

  MESSAGE("erasing an element that is not present");
  CHECK(!uut.erase("this:key:has:a:long:prefix:"));
  CHECK(!uut.erase("this:key:has:a:long:prefix:32"));
  CHECK(uut.contains("this:key:has:a:long:prefix:3"));
  CHECK(uut.contains("this:key:has:a:long:common:prefix:2"));
  CHECK(uut.contains("this:key:has:a:long:common:prefix:1"));

  MESSAGE("erasing an element once that has been inserted twice");
  CHECK(!uut.erase("this:key:has:a:long:prefix:3"));
  CHECK(uut.contains("this:key:has:a:long:prefix:3"));
  CHECK(uut.contains("this:key:has:a:long:common:prefix:2"));
  CHECK(uut.contains("this:key:has:a:long:common:prefix:1"));

  MESSAGE("erasing an element twice that has been inserted twice");
  CHECK(uut.erase("this:key:has:a:long:prefix:3"));
  CHECK(!uut.contains("this:key:has:a:long:prefix:3"));
  CHECK(uut.contains("this:key:has:a:long:common:prefix:2"));
  CHECK(uut.contains("this:key:has:a:long:common:prefix:1"));

  MESSAGE("erasing another element");
  CHECK(uut.erase("this:key:has:a:long:common:prefix:2"));
  CHECK(!uut.contains("this:key:has:a:long:prefix:3"));
  CHECK(!uut.contains("this:key:has:a:long:common:prefix:2"));
  CHECK(uut.contains("this:key:has:a:long:common:prefix:1"));

  MESSAGE("erasing the last element");
  CHECK(uut.erase("this:key:has:a:long:common:prefix:1"));
  CHECK(!uut.contains("this:key:has:a:long:prefix:3"));
  CHECK(!uut.contains("this:key:has:a:long:common:prefix:2"));
  CHECK(!uut.contains("this:key:has:a:long:common:prefix:1"));
  CHECK(uut.empty());
}
