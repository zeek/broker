#include "broker/data.hh"

#include "broker/broker-test.test.hh"
#include "broker/format/bin.hh"

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>

#include "broker/convert.hh"

using namespace broker;
using namespace std::literals;

static_assert(std::is_same_v<boolean, bool>);
static_assert(std::is_same_v<integer, int64_t>);
static_assert(std::is_same_v<count, uint64_t>);
static_assert(std::is_same_v<real, double>);

// Note: the `CHECK(opt) && opt` pattern exists only because clang-tidy doesn't
//       recognize that `CHECK(opt)` already checks that `opt` contains a value.
//       The `&& opt` is completely redundant, but it suppresses the false
//       positives.

TEST(timespan) {
  auto s = timespan{42};
  CHECK(std::chrono::nanoseconds{42} == s);
}

TEST(timestamp) {
  auto ts = timestamp{timespan{42}};
  CHECK(ts.time_since_epoch() == timespan{42});
}

TEST(enum) {
  auto e = enum_value{"foo"};
  CHECK_EQUAL(e.name, "foo");
}

TEST(address) {
  address a;
  // Default-constructed addresses are considered IPv6.
  CHECK(!a.is_v4());
  CHECK(a.is_v6());
  MESSAGE("parsing");
  if (auto opt = to<address>("dead::beef"); CHECK(opt) && opt) {
    CHECK(!opt->is_v4());
    CHECK(opt->is_v6());
  }
  if (auto opt = to<address>("1.2.3.4"); CHECK(opt) && opt) {
    CHECK(opt->is_v4());
    CHECK(!opt->is_v6());
    MESSAGE("printing");
    CHECK_EQUAL(to_string(*opt), "1.2.3.4");
    MESSAGE("masking");
    CHECK(opt->mask(96 + 16));
    CHECK_EQUAL(to_string(*opt), "1.2.0.0");
  }
}

TEST(port) {
  port p;
  CHECK_EQUAL(p.number(), 0u);
  CHECK(p.type() == port::protocol::unknown);
  p = {80, port::protocol::tcp};
  MESSAGE("parsing");
  if (auto opt = to<port>("8/icmp"); CHECK(opt) && opt) {
    CHECK_EQUAL(*opt, port(8, port::protocol::icmp));
  }
  if (auto opt = to<port>("42/nonsense"); CHECK(opt) && opt) {
    CHECK_EQUAL(*opt, port(42, port::protocol::unknown));
    MESSAGE("printing");
    CHECK_EQUAL(to_string(p), "80/tcp");
  }
  p = {0, port::protocol::unknown};
  CHECK_EQUAL(to_string(p), "0/?");
}

TEST(subnet) {
  subnet sn;
  CHECK_EQUAL(sn.length(), 0u);
  CHECK_EQUAL(to_string(sn), "::/0");
  auto a = to<address>("1.2.3.4");
  auto b = to<address>("1.2.3.0");
  if (CHECK(a && b) && a && b) {
    sn = {*a, 24};
    CHECK_EQUAL(sn.length(), 24u);
    CHECK_EQUAL(sn.network(), *b);
  }
}

TEST(data - construction) {
  MESSAGE("default construction");
  data d;
  CHECK(get_if<none>(&d));
}

TEST(data - assignment) {
  data d;
  d = 42;
  auto i = get_if<integer>(&d);
  REQUIRE(i);
  CHECK_EQUAL(*i, 42);
  d = data{7};
  i = get_if<integer>(&d);
  CHECK_EQUAL(*i, 7);
  d = "foo";
  auto s = get_if<std::string>(&d);
  REQUIRE(s);
  CHECK_EQUAL(*s, "foo");
}

TEST(data - relational operators) {
  CHECK_NOT_EQUAL(data{true}, data{false});
  CHECK_NOT_EQUAL(data{1}, data{true});
  CHECK_NOT_EQUAL(data{-1}, data{1});
  CHECK_NOT_EQUAL(data{1}, data{1u});
  CHECK_NOT_EQUAL(data{1.111}, data{1.11});
  CHECK_EQUAL(data{1.111}, data{1.111});
}

TEST(data - vector) {
  vector v{42, 43, 44};
  REQUIRE_EQUAL(v.size(), 3u);
  CHECK_EQUAL(v[1], data{43});
  CHECK_EQUAL(to_string(v), "(42, 43, 44)");
}

TEST(data - set) {
  set s{"foo", "bar", "baz", "foo"};
  CHECK_EQUAL(s.size(), 3u); // one duplicate
  CHECK(s.find("bar") != s.end());
  CHECK(s.find("qux") == s.end());
  CHECK_EQUAL(to_string(s), "{bar, baz, foo}");
}

TEST(data - table) {
  table t{{"foo", 42}, {"bar", 43}, {"baz", 44}};
  auto i = t.find("foo");
  REQUIRE(i != t.end());
  CHECK_EQUAL(i->second, data{42});
  CHECK_EQUAL(to_string(t), "{bar -> 43, baz -> 44, foo -> 42}");
}

namespace {

template <class T>
data do_deserialize(T&& arg) {
  auto input = data{std::forward<T>(arg)};
  std::vector<std::byte> buf;
  format::bin::v1::encode(input, std::back_inserter(buf));
  data result;
  if (!result.deserialize(buf.data(), buf.size())) {
    FAIL("deserialization failed");
  }
  return result;
}

address addr(const std::string& str) {
  address result;
  if (!convert(str, result))
    FAIL("conversion to address failed for " << str);
  return result;
}

subnet snet(const std::string& str) {
  subnet result;
  if (!convert(str, result))
    FAIL("conversion to subnet failed for " << str);
  return result;
}

} // namespace

#define CHECK_ROUNDTRIP(val) CHECK_EQ(do_deserialize(val), data{val})

TEST(deserialization) {
  CHECK_ROUNDTRIP(data{});
  CHECK_ROUNDTRIP(true);
  CHECK_ROUNDTRIP(count{42});
  CHECK_ROUNDTRIP(integer{42});
  CHECK_ROUNDTRIP(real{1.2});
  CHECK_ROUNDTRIP("FooBar"s);
  CHECK_ROUNDTRIP(addr("192.168.9.8"));
  CHECK_ROUNDTRIP(snet("192.168.9.0/24"));
  CHECK_ROUNDTRIP(port(8080, port::protocol::tcp));
  CHECK_ROUNDTRIP(timestamp{timespan{12345}});
  CHECK_ROUNDTRIP(timespan{12345});
  CHECK_ROUNDTRIP(enum_value{"FooBar"});
  CHECK_ROUNDTRIP(set{});
  CHECK_ROUNDTRIP((set{data{count{1}}, data{count{2}}}));
  CHECK_ROUNDTRIP(vector{});
  CHECK_ROUNDTRIP((vector{data{count{1}}, data{count{2}}}));
  CHECK_ROUNDTRIP((vector{data{vector{}}, data{count{2}}}));
  CHECK_ROUNDTRIP((vector{
    data{vector{data{count{1}}, data{count{2}}}},
    data{count{2}},
  }));
  CHECK_ROUNDTRIP(vector{data{set{}}});
  CHECK_ROUNDTRIP((vector{
    data{set{data{count{1}}, data{count{2}}}},
    data{count{2}},
  }));
  CHECK_ROUNDTRIP(table{});
  CHECK_ROUNDTRIP((table{
    {data{"a"}, data{count{1}}},
    {data{"b"}, data{count{2}}},
  }));
  CHECK_ROUNDTRIP((table{
    {vector{data{count{1}}, data{count{2}}}, vector{data{count{3}}}},
    {data{"b"}, data{count{2}}},
  }));
}
