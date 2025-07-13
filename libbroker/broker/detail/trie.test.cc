#include "broker/detail/trie.hh"

#include "broker/broker-test.test.hh"
#include "broker/logger.hh"

using namespace broker;
using namespace std::literals;

namespace {

std::vector<std::string> sorted(std::vector<std::string> keys) {
  std::sort(keys.begin(), keys.end());
  return keys;
}

void render(const detail::trie::node* pos, std::string& result,
            int indent = 0) {
  auto inserter = std::back_inserter(result);
  auto prefix = std::string(indent, ' ');
  uint8_t ch = 0;
  for (auto& seg : pos->segments) {
    if (!seg) {
      ch += detail::trie::segment_size;
      continue;
    }
    for (auto& child : seg->children) {
      if (!child) {
        ++ch;
        continue;
      }
      char buf[] = {static_cast<char>(ch), '\0'};
      detail::fmt_to(inserter, "{}'{}':\n", prefix, buf);
      detail::fmt_to(inserter, "{}  'value': {},\n", prefix, child->val);
      detail::fmt_to(inserter, "{}  'compressed': {},\n", prefix,
                     child->compressed);
      if (child->key_data) {
        detail::fmt_to(inserter, "{}  'key': '{}',\n", prefix, child->key());
      }
      if (child->has_children()) {
        detail::fmt_to(inserter, "{}  'children': [\n", prefix);
        render(child.get(), result, indent + 4);
        detail::fmt_to(inserter, "{}  ],\n", prefix);
      } else {
        detail::fmt_to(inserter, "{}  'children': [],\n", prefix);
      }
      ++ch;
    }
  }
}

std::string render(const detail::trie& uut, int indent = 0) {
  std::string result;
  result += '\n';
  render(uut.root(), result, indent);
  result.insert(result.end(), indent, ' ');
  return result;
}

} // namespace

#define SECTION(str) MESSAGE(str);

TEST(node splitting) {
  SECTION("inserting a key that is a prefix of an existing key") {
    detail::trie uut;
    uut.insert("foobar");
    uut.insert("foo");
    CHECK_EQ(uut.size(), 2);
    CHECK(uut.contains("foo"));
    CHECK(uut.contains("foobar"));
    CHECK_EQ(render(uut, 6), R"__(
      'f':
        'value': 0,
        'compressed': 0,
        'children': [
          'o':
            'value': 0,
            'compressed': 0,
            'children': [
              'o':
                'value': 1,
                'compressed': 0,
                'key': 'foo',
                'children': [
                  'b':
                    'value': 1,
                    'compressed': 1,
                    'key': 'foobar',
                    'children': [],
                ],
            ],
        ],
      )__");
  }
  SECTION("inserting a key that contains an existing key") {
    detail::trie uut;
    uut.insert("foobar");
    uut.insert("foo");
    CHECK_EQ(uut.size(), 2);
    CHECK(uut.contains("foo"));
    CHECK(uut.contains("foobar"));
    CHECK_EQ(render(uut, 6), R"__(
      'f':
        'value': 0,
        'compressed': 0,
        'children': [
          'o':
            'value': 0,
            'compressed': 0,
            'children': [
              'o':
                'value': 1,
                'compressed': 0,
                'key': 'foo',
                'children': [
                  'b':
                    'value': 1,
                    'compressed': 1,
                    'key': 'foobar',
                    'children': [],
                ],
            ],
        ],
      )__");
  }
  SECTION("inserting a key that adds a single character to an existing key") {
    detail::trie uut;
    uut.insert("abc");
    uut.insert("abcd");
    CHECK_EQ(uut.size(), 2);
    CHECK(uut.contains("abc"));
    CHECK(uut.contains("abcd"));
    CHECK_EQ(render(uut, 6), R"__(
      'a':
        'value': 0,
        'compressed': 0,
        'children': [
          'b':
            'value': 0,
            'compressed': 0,
            'children': [
              'c':
                'value': 1,
                'compressed': 0,
                'key': 'abc',
                'children': [
                  'd':
                    'value': 1,
                    'compressed': 0,
                    'key': 'abcd',
                    'children': [],
                ],
            ],
        ],
      )__");
  }
  SECTION("inserting a key that shares a prefix with an existing key") {
    detail::trie uut;
    uut.insert("/my/foo");
    uut.insert("/my/bar");
    CHECK_EQ(uut.size(), 2);
    CHECK(uut.contains("/my/foo"));
    CHECK(uut.contains("/my/bar"));
    CHECK_EQ(render(uut, 6), R"__(
      '/':
        'value': 0,
        'compressed': 0,
        'children': [
          'm':
            'value': 0,
            'compressed': 0,
            'children': [
              'y':
                'value': 0,
                'compressed': 0,
                'children': [
                  '/':
                    'value': 0,
                    'compressed': 0,
                    'children': [
                      'b':
                        'value': 1,
                        'compressed': 1,
                        'key': '/my/bar',
                        'children': [],
                      'f':
                        'value': 1,
                        'compressed': 1,
                        'key': '/my/foo',
                        'children': [],
                    ],
                ],
            ],
        ],
      )__");
  }
  SECTION("inserting a key that is a prefix of a twice inserted existing key") {
    detail::trie uut;
    uut.insert("foobar");
    uut.insert("foobar");
    CHECK_EQ(uut.size(), 1);
    CHECK_EQ(uut.value("foobar"), 2);
    uut.insert("foo");
    CHECK_EQ(uut.size(), 2);
    CHECK_EQ(uut.value("foobar"), 2);
    CHECK_EQ(uut.value("foo"), 1);
    CHECK(uut.contains("foo"));
    CHECK(uut.contains("foobar"));
    CHECK_EQ(render(uut, 6), R"__(
      'f':
        'value': 0,
        'compressed': 0,
        'children': [
          'o':
            'value': 0,
            'compressed': 0,
            'children': [
              'o':
                'value': 1,
                'compressed': 0,
                'key': 'foo',
                'children': [
                  'b':
                    'value': 2,
                    'compressed': 1,
                    'key': 'foobar',
                    'children': [],
                ],
            ],
        ],
      )__");
  }
  SECTION("inserting a key that is a suffix of a twice inserted existing key") {
    detail::trie uut;
    uut.insert("foobar");
    uut.insert("foobar");
    CHECK_EQ(uut.value("foobar"), 2);
    uut.insert("foo");
    CHECK_EQ(uut.value("foobar"), 2);
    CHECK_EQ(uut.value("foo"), 1);
    CHECK(uut.contains("foo"));
    CHECK(uut.contains("foobar"));
    CHECK_EQ(render(uut, 6), R"__(
      'f':
        'value': 0,
        'compressed': 0,
        'children': [
          'o':
            'value': 0,
            'compressed': 0,
            'children': [
              'o':
                'value': 1,
                'compressed': 0,
                'key': 'foo',
                'children': [
                  'b':
                    'value': 2,
                    'compressed': 1,
                    'key': 'foobar',
                    'children': [],
                ],
            ],
        ],
      )__");
  }
  SECTION("inserting a key that shares a prefix with a twice inserted key") {
    detail::trie uut;
    uut.insert("/my/foo");
    uut.insert("/my/foo");
    CHECK_EQ(uut.value("/my/foo"), 2);
    uut.insert("/my/bar");
    CHECK_EQ(uut.value("/my/foo"), 2);
    CHECK_EQ(uut.value("/my/bar"), 1);
    CHECK(uut.contains("/my/foo"));
    CHECK(uut.contains("/my/bar"));
    CHECK_EQ(render(uut, 6), R"__(
      '/':
        'value': 0,
        'compressed': 0,
        'children': [
          'm':
            'value': 0,
            'compressed': 0,
            'children': [
              'y':
                'value': 0,
                'compressed': 0,
                'children': [
                  '/':
                    'value': 0,
                    'compressed': 0,
                    'children': [
                      'b':
                        'value': 1,
                        'compressed': 1,
                        'key': '/my/bar',
                        'children': [],
                      'f':
                        'value': 2,
                        'compressed': 1,
                        'key': '/my/foo',
                        'children': [],
                    ],
                ],
            ],
        ],
      )__");
  }
  SECTION("inserting preserves the values") {
    SECTION("new key is a prefix of old key") {
      detail::trie uut;
      uut.insert("foobar");
      uut.insert("foobar");
      CHECK_EQ(uut.value("foobar"), 2);
      uut.insert("foo");
      CHECK_EQ(uut.value("foo"), 1);
      CHECK_EQ(uut.value("foobar"), 2);
    }
    SECTION("old key is a prefix of new key") {
      detail::trie uut;
      uut.insert("foo");
      uut.insert("foo");
      CHECK_EQ(uut.value("foo"), 2);
      uut.insert("foobar");
      CHECK_EQ(uut.value("foo"), 2);
      CHECK_EQ(uut.value("foobar"), 1);
    }
    SECTION("neither is a prefix of the other") {
      detail::trie uut;
      uut.insert("foobar");
      uut.insert("foobar");
      CHECK_EQ(uut.value("foobar"), 2);
      uut.insert("foobaz");
      CHECK_EQ(uut.value("foobar"), 2);
      CHECK_EQ(uut.value("foobaz"), 1);
      CHECK(!uut.contains("fooba"));
    }
  }
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
      std::cout << "Failed prefix lookup: " << key1.substr(0, i) << '\n';
    }
  }
  for (auto i = 1; i < key2.size(); ++i) {
    if (!CHECK(uut.has_prefix(key2.substr(0, i)))) {
      std::cout << "Failed prefix lookup: " << key2.substr(0, i) << '\n';
    }
  }
  for (auto i = 1; i < key3.size(); ++i) {
    if (!CHECK(uut.has_prefix(key3.substr(0, i)))) {
      std::cout << "Failed prefix lookup: " << key3.substr(0, i) << '\n';
    }
  }
  CHECK(uut.has_prefix(key1));
  CHECK(uut.has_prefix(key2));
  CHECK(uut.has_prefix(key3));
  CHECK(!uut.has_prefix("that"));
  CHECK(!uut.has_prefix("this:key:has:a:long:prefix:31"));
}

TEST(erasing elements) {
  SECTION("erasing an element that is not present") {
    detail::trie uut;
    CHECK(!uut.erase("foo"));
    uut.insert("foobar");
    CHECK(!uut.erase("foo"));
    CHECK(!uut.erase("fooba"));
    CHECK(!uut.erase("foobarz"));
    CHECK(uut.contains("foobar"));
  }
  SECTION("erasing a key that is a prefix of another key") {
    detail::trie uut;
    uut.insert("foobar");
    uut.insert("foo");
    CHECK(uut.erase("foo"));
    CHECK(!uut.contains("foo"));
    CHECK(uut.contains("foobar"));
    CHECK_EQ(render(uut, 6), R"__(
      'f':
        'value': 0,
        'compressed': 0,
        'children': [
          'o':
            'value': 0,
            'compressed': 0,
            'children': [
              'o':
                'value': 0,
                'compressed': 0,
                'children': [
                  'b':
                    'value': 1,
                    'compressed': 1,
                    'key': 'foobar',
                    'children': [],
                ],
            ],
        ],
      )__");
  }
  SECTION("erasing a key that is a suffix of an existing key") {
    detail::trie uut;
    uut.insert("foobar");
    uut.insert("foo");
    CHECK(uut.erase("foobar"));
    CHECK(uut.contains("foo"));
    CHECK(!uut.contains("foobar"));
    CHECK_EQ(render(uut, 6), R"__(
      'f':
        'value': 0,
        'compressed': 0,
        'children': [
          'o':
            'value': 0,
            'compressed': 0,
            'children': [
              'o':
                'value': 1,
                'compressed': 0,
                'key': 'foo',
                'children': [],
            ],
        ],
      )__");
  }
  SECTION("inserting a key that shares a prefix with an existing key") {
    detail::trie uut;
    uut.insert("/my/foo");
    uut.insert("/my/bar");
    CHECK(uut.erase("/my/foo"));
    CHECK(!uut.contains("/my/foo"));
    CHECK(uut.contains("/my/bar"));
    CHECK_EQ(render(uut, 6), R"__(
      '/':
        'value': 0,
        'compressed': 0,
        'children': [
          'm':
            'value': 0,
            'compressed': 0,
            'children': [
              'y':
                'value': 0,
                'compressed': 0,
                'children': [
                  '/':
                    'value': 0,
                    'compressed': 0,
                    'children': [
                      'b':
                        'value': 1,
                        'compressed': 1,
                        'key': '/my/bar',
                        'children': [],
                    ],
                ],
            ],
        ],
      )__");
  }
  SECTION("erasing the last element") {
    detail::trie uut;
    uut.insert("this:key:has:a:long:prefix:3");
    uut.insert("this:key:has:a:long:prefix:3");
    uut.insert("this:key:has:a:long:common:prefix:2");
    uut.insert("this:key:has:a:long:common:prefix:1");
    CHECK(!uut.erase("this:key:has:a:long:prefix:3"));
    CHECK(uut.erase("this:key:has:a:long:prefix:3"));
    CHECK(uut.erase("this:key:has:a:long:common:prefix:2"));
    CHECK(uut.erase("this:key:has:a:long:common:prefix:1"));
    CHECK(uut.empty());
    CHECK(!uut.root()->has_children());
  }
  SECTION("erasing the last key from a chain of keys") {
    detail::trie uut;
    uut.insert("abc");
    CHECK_EQ(uut.value("abc"), 1);
    uut.insert("abc");
    CHECK_EQ(uut.value("abc"), 2);
    uut.insert("abc");
    CHECK_EQ(uut.value("abc"), 3);
    uut.insert("abd"); // Trigger node splitting.
    CHECK_EQ(uut.value("abc"), 3);
    CHECK_EQ(uut.value("abd"), 1);
    uut.insert("abce"); // Add a child to the 'abc' node
    CHECK_EQ(uut.value("abc"), 3);
    CHECK_EQ(uut.value("abce"), 1);
    CHECK_EQ(uut.value("abd"), 1);
    CHECK_EQ(render(uut, 6), R"__(
      'a':
        'value': 0,
        'compressed': 0,
        'children': [
          'b':
            'value': 0,
            'compressed': 0,
            'children': [
              'c':
                'value': 3,
                'compressed': 0,
                'key': 'abc',
                'children': [
                  'e':
                    'value': 1,
                    'compressed': 0,
                    'key': 'abce',
                    'children': [],
                ],
              'd':
                'value': 1,
                'compressed': 0,
                'key': 'abd',
                'children': [],
            ],
        ],
      )__");
    CHECK(!uut.erase("abc")); // false because 'abc' was inserted three times
    CHECK(uut.contains("abc"));
    CHECK(uut.contains("abce"));
    // Now erase the child first, then the parent
    CHECK(uut.erase("abce"));
    CHECK(!uut.erase("abc")); // Should still return false because value > 1
    CHECK(uut.erase("abc"));  // Should now succeed
    CHECK(!uut.contains("abc"));
    CHECK_EQ(render(uut, 6), R"__(
      'a':
        'value': 0,
        'compressed': 0,
        'children': [
          'b':
            'value': 0,
            'compressed': 0,
            'children': [
              'd':
                'value': 1,
                'compressed': 0,
                'key': 'abd',
                'children': [],
            ],
        ],
      )__");
  }
}

TEST(keys) {
  detail::trie uut;
  CHECK_EQ(uut.keys(), std::vector<std::string>{});
  uut.insert("prefix1:key1");
  uut.insert("prefix1:key2");
  uut.insert("prefix1:key2");
  uut.insert("prefix2:key1");
  uut.insert("prefix2:key2");
  uut.insert("prefix3:key1");
  CHECK_EQ(uut.keys(),
           std::vector({"prefix1:key1"s, "prefix1:key2"s, "prefix2:key1"s,
                        "prefix2:key2"s, "prefix3:key1"s}));
}

TEST(each insert requires one erase) {
  detail::trie uut;
  uut.insert("foo");
  CHECK(uut.contains("foo"));
  uut.insert("foo");
  uut.insert("foobar");
  uut.insert("foo");
  CHECK_EQ(uut.size(), 2);
  CHECK(!uut.erase("foo"));
  CHECK(uut.contains("foo"));
  CHECK_EQ(uut.size(), 2);
  CHECK(!uut.erase("foo"));
  CHECK(uut.contains("foo"));
  CHECK_EQ(uut.size(), 2);
  CHECK(uut.erase("foo"));
  CHECK(!uut.contains("foo"));
  CHECK_EQ(uut.size(), 1);
  CHECK(uut.erase("foobar"));
  CHECK(uut.empty());
}

TEST(empty string handling) {
  SECTION("empty string insert is a no-op") {
    detail::trie uut;
    uut.insert("");
    CHECK(uut.empty());
    CHECK(!uut.contains(""));
    CHECK(uut.has_prefix(""));
  }
  SECTION("empty string erase is a no-op") {
    detail::trie uut;
    CHECK(!uut.erase(""));
    uut.insert("foo");
    CHECK(!uut.erase(""));
    CHECK(uut.contains("foo"));
  }
  SECTION("the empty string is a prefix of any key") {
    detail::trie uut;
    CHECK(uut.has_prefix(""));
    uut.insert("foo");
    CHECK(uut.has_prefix(""));
  }
}

TEST(edge cases and error conditions) {
  detail::trie uut;

  // Test inserting a key that will trigger the new_node_at path
  // This requires creating a scenario where we need to split nodes
  uut.insert("abc");
  uut.insert("abd"); // This should create a new node at 'd'

  // Test child not found in erase
  CHECK(!uut.erase("xyz")); // Should return false when child not found

  // Test child not found in has_prefix
  CHECK(!uut.has_prefix("xyz")); // Should return false when child not found

  // Test final return false in erase
  uut.insert("test");
  CHECK(!uut.erase("testx")); // Should return false for non-existent key
}

/*

TEST(prefix matching edge cases) {
  detail::trie uut;

  // Test prefix matching with various scenarios
  uut.insert("test");
  uut.insert("testing");
  uut.insert("tested");

  // Test prefix that exists
  CHECK(uut.has_prefix("test"));
  CHECK(uut.has_prefix("tes"));
  CHECK(uut.has_prefix("te"));
  CHECK(uut.has_prefix("t"));

  // Test prefix that doesn't exist
  CHECK(!uut.has_prefix("xyz"));
  CHECK(!uut.has_prefix("testx"));

  // Test exact match as prefix
  CHECK(uut.has_prefix("test"));
  CHECK(uut.has_prefix("testing"));
  CHECK(uut.has_prefix("tested"));
}

TEST(multiple insertions and complex scenarios) {
  detail::trie uut;

  // Test inserting the same key multiple times
  uut.insert("key");
  uut.insert("key");
  uut.insert("key");

  CHECK_EQ(uut.size(), 1);
  CHECK(uut.contains("key"));

  // Test erasing multiple times
  CHECK(!uut.erase("key")); // First erase should return false (count > 1)
  CHECK(!uut.erase("key")); // Second erase should return false (count > 1)
  CHECK(uut.erase("key"));  // Third erase should return true (count == 1)
  CHECK(!uut.contains("key"));

  // Test inserting keys that will trigger various code paths
  uut.insert("prefix");
  uut.insert("prefix:key1");
  uut.insert("prefix:key2");

  // Test that prefix matching works correctly
  CHECK(uut.has_prefix("prefix"));
  CHECK(uut.has_prefix("prefix:"));
  CHECK(uut.has_prefix("prefix:k"));
  CHECK(!uut.has_prefix("prefix:key3"));

  // Test erasing in reverse order to trigger cleanup
  CHECK(uut.erase("prefix:key2"));
  CHECK(uut.erase("prefix:key1"));
  CHECK(uut.erase("prefix"));
  CHECK(uut.empty());
}

TEST(specific uncovered lines) {
  detail::trie uut;

  // Test line 133-134: Setting key and incrementing value in insert
  // This happens when we reach the end of a string and the node has no value
  // yet
  uut.insert("x");
  CHECK(uut.contains("x"));

  // Test line 208-210: Path handling in erase
  // Create a scenario where we need to clean up the path after erasing
  uut.insert("a");
  uut.insert("ab");
  uut.insert("abc");

  // Erase the deepest node first, which should trigger path cleanup
  CHECK(uut.erase("abc"));
  CHECK(!uut.contains("abc"));
  CHECK(uut.contains("ab"));

  // Test line 213: Final return false in erase
  // This happens when we reach the end of string but the key doesn't match
  uut.insert("abcx");
  CHECK(
    !uut.erase("abc")); // Should return false because key doesn't match exactly

  // Test line 165: do_erase lambda early return
  // This happens when a node still has children after clearing
  uut.insert("abcy");
  CHECK(
    !uut.erase("abc")); // Should return false because 'abc' still has children
  CHECK(uut.contains("abc"));
  CHECK(uut.contains("abcx"));
  CHECK(uut.contains("abcy"));

  // Now erase the children first
  CHECK(uut.erase("abcx"));
  CHECK(uut.erase("abcy"));
  CHECK(uut.erase("abc")); // Should now succeed
  CHECK(!uut.contains("abc"));
}

TEST(compressed node scenarios) {
  detail::trie uut;

  // Test scenarios involving compressed nodes
  uut.insert("test");
  uut.insert("testing");

  // Test that "test" is a compressed node but also a prefix
  CHECK(uut.contains("test"));
  CHECK(uut.has_prefix("test"));

  // Test erasing a compressed node that's also a prefix
  CHECK(uut.erase("test"));
  CHECK(!uut.contains("test"));
  CHECK(uut.contains("testing"));

  // Test inserting a key that shares a prefix with an existing compressed
  uut.insert("test");
  uut.insert("tested");

  // This should trigger the node splitting logic
  CHECK(uut.contains("test"));
  CHECK(uut.contains("testing"));
  CHECK(uut.contains("tested"));
}

TEST(deep trie structures) {
  detail::trie uut;

  // Create a deep trie to test various edge cases
  uut.insert("a");
  uut.insert("aa");
  uut.insert("aaa");
  uut.insert("aaaa");
  uut.insert("aaaaa");

  // Test erasing from deepest to shallowest
  CHECK(uut.erase("aaaaa"));
  CHECK(!uut.contains("aaaaa"));
  CHECK(uut.contains("aaaa"));

  CHECK(uut.erase("aaaa"));
  CHECK(!uut.contains("aaaa"));
  CHECK(uut.contains("aaa"));

  CHECK(uut.erase("aaa"));
  CHECK(!uut.contains("aaa"));
  CHECK(uut.contains("aa"));

  CHECK(uut.erase("aa"));
  CHECK(!uut.contains("aa"));
  CHECK(uut.contains("a"));

  CHECK(uut.erase("a"));
  CHECK(!uut.contains("a"));
  CHECK(uut.empty());
}

TEST(complex branching scenarios) {
  detail::trie uut;

  // Create a complex branching structure
  uut.insert("cat");
  uut.insert("car");
  uut.insert("cab");
  uut.insert("cap");

  // Test that all keys are present
  CHECK(uut.contains("cat"));
  CHECK(uut.contains("car"));
  CHECK(uut.contains("cab"));
  CHECK(uut.contains("cap"));

  // Test prefix matching
  CHECK(uut.has_prefix("ca"));
  CHECK(uut.has_prefix("cat"));
  CHECK(uut.has_prefix("car"));
  CHECK(!uut.has_prefix("caa"));

  // Test erasing in various orders
  CHECK(uut.erase("cat"));
  CHECK(!uut.contains("cat"));
  CHECK(uut.contains("car"));
  CHECK(uut.contains("cab"));
  CHECK(uut.contains("cap"));

  CHECK(uut.erase("car"));
  CHECK(!uut.contains("car"));
  CHECK(uut.contains("cab"));
  CHECK(uut.contains("cap"));

  CHECK(uut.erase("cab"));
  CHECK(!uut.contains("cab"));
  CHECK(uut.contains("cap"));

  CHECK(uut.erase("cap"));
  CHECK(!uut.contains("cap"));
  CHECK(uut.empty());
}

TEST(debug node splitting) {
  detail::trie uut;

  // Insert "abc" three times
  uut.insert("abc");
  uut.insert("abc");
  uut.insert("abc");

  // Check that "abc" is present
  CHECK(uut.contains("abc"));
  CHECK_EQ(uut.size(), 1);

  // Insert "abd" to trigger node splitting
  uut.insert("abd");

  // Check both keys are present
  CHECK(uut.contains("abc"));
  CHECK(uut.contains("abd"));
  CHECK_EQ(uut.size(), 2);

  // Insert "abce" to add a child to "abc"
  uut.insert("abce");

  // Check all keys are present
  CHECK(uut.contains("abc"));
  CHECK(uut.contains("abd"));
  CHECK(uut.contains("abce"));
  CHECK_EQ(uut.size(), 3);

  // Now try to erase "abc" - should return false because val > 1
  CHECK(!uut.erase("abc"));

  // "abc" should still be present
  CHECK(uut.contains("abc"));
  CHECK(uut.contains("abd"));
  CHECK(uut.contains("abce"));
  CHECK_EQ(uut.size(), 3);
}

TEST(minimal node splitting bug) {
  detail::trie uut;

  // Insert "abc" to create a compressed node
  uut.insert("abc");
  CHECK(uut.contains("abc"));

  // Insert "abd" to trigger first node splitting
  uut.insert("abd");
  CHECK(uut.contains("abc"));
  CHECK(uut.contains("abd"));

  // Insert "abce" - this should trigger another node splitting
  // but it's causing the "abc" key to be lost
  uut.insert("abce");
  CHECK(uut.contains("abc")); // This is failing!
  CHECK(uut.contains("abd"));
  CHECK(uut.contains("abce"));
}

TEST(debug foo foobar) {
  detail::trie uut;

  // Insert "foo" multiple times
  uut.insert("foo");
  uut.insert("foo");
  uut.insert("foo");

  // Check that "foo" is present
  CHECK(uut.contains("foo"));
  CHECK_EQ(uut.size(), 1);

  // Insert "foobar" - this should trigger node splitting
  uut.insert("foobar");

  // Check both keys are present
  CHECK(uut.contains("foo"));
  CHECK(uut.contains("foobar"));
  CHECK_EQ(uut.size(), 2);

  // Now try to erase "foo" - should return false because val > 1
  CHECK(!uut.erase("foo"));

  // "foo" should still be present
  CHECK(uut.contains("foo"));
  CHECK(uut.contains("foobar"));
  CHECK_EQ(uut.size(), 2);
}

*/
