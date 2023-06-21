#define SUITE data_view

#include "broker/data_view.hh"
#include "broker/data.hh"

#include "test.hh"

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>

#include "broker/convert.hh"

#include <caf/detail/append_hex.hpp>

#define SUBTEST(name)                                                          \
  MESSAGE(name);                                                               \
  if (true)

using namespace broker;
using namespace std::literals;

namespace {

using byte_buffer = caf::byte_buffer;

std::string to_hex(const byte_buffer& buf) {
  std::string result;
  caf::detail::append_hex(result, buf.data(), buf.size());
  return result;
}

template <class... Ts>
byte_buffer make_bytes(Ts... xs) {
  return {static_cast<caf::byte>(xs)...};
}

template <class T>
data_view parse_bytes_buf(const std::vector<T>& bytes) {
  auto result = data_view::from(bytes);
  if (result)
    return *result;
  return data_view{};
}

template <class... Ts>
data_view parse_bytes(Ts... xs) {
  return parse_bytes_buf(make_bytes(xs...));
}

bool convert(const byte_buffer& bytes, data& value) {
  caf::binary_deserializer source{nullptr, bytes};
  return source.apply(value);
}

bool convert(const data& value, byte_buffer& bytes) {
  caf::binary_serializer sink{nullptr, bytes};
  return sink.apply(value);
}

byte_buffer to_bytes(const data& value) {
  byte_buffer result;
  REQUIRE(convert(value, result));
  return result;
}

using type = data::type;

struct fixture {
  address localhost;
  subnet localnet;

  fixture() {
    convert("127.0.0.1"s, localhost);
    convert("2001:db8::/32"s, localnet);
  }
};

} // namespace

FIXTURE_SCOPE(data_view_tests, fixture)

TEST(default construction) {
  CHECK(data_view{}.is_none());
  CHECK_EQ(data_view{}, data_view{});
}

TEST(parsing a none) {
  CHECK(data_view::from(make_bytes(type::none)));
  CHECK(!data_view::from(make_bytes(type::none, 1)));
  if (auto val = data_view::from(make_bytes(type::none)); CHECK(val))
    CHECK(val->is_none());
}

TEST(parsing a boolean) {
  CHECK(parse_bytes(type::boolean).is_none()); // not enough bytes
  CHECK(parse_bytes(type::boolean, 1, 1).is_none()); // trailing bytes
  CHECK_EQ(parse_bytes(type::boolean, 1).to_boolean(false), true);
  CHECK_EQ(parse_bytes(type::boolean, 0).to_boolean(true), false);
}

TEST(parsing a count) {
  CHECK(parse_bytes(type::count).is_none());
  CHECK(parse_bytes(type::count, 1).is_none());
  CHECK(parse_bytes(type::count, 1, 2).is_none());
  CHECK(parse_bytes(type::count, 1, 2, 3).is_none());
  CHECK(parse_bytes(type::count, 1, 2, 3, 4).is_none());
  CHECK(parse_bytes(type::count, 1, 2, 3, 4, 5).is_none());
  CHECK(parse_bytes(type::count, 1, 2, 3, 4, 5, 6, 7).is_none());
  CHECK(parse_bytes(type::count, 1, 2, 3, 4, 5, 6, 7, 8, 9).is_none());
  CHECK_EQ(parse_bytes(type::count, 1, 2, 3, 4, 5, 6, 7, 8).to_count(),
           0x0102030405060708);
}

TEST(parsing an integer) {
  CHECK(parse_bytes(type::integer).is_none());
  CHECK(parse_bytes(type::integer, 1).is_none());
  CHECK(parse_bytes(type::integer, 1, 2).is_none());
  CHECK(parse_bytes(type::integer, 1, 2, 3).is_none());
  CHECK(parse_bytes(type::integer, 1, 2, 3, 4).is_none());
  CHECK(parse_bytes(type::integer, 1, 2, 3, 4, 5).is_none());
  CHECK(parse_bytes(type::integer, 1, 2, 3, 4, 5, 6, 7).is_none());
  CHECK(parse_bytes(type::integer, 1, 2, 3, 4, 5, 6, 7, 8, 9).is_none());
  CHECK_EQ(parse_bytes(type::integer, 1, 2, 3, 4, 5, 6, 7, 8).to_integer(),
           0x0102030405060708);
}

TEST(parsing an real) {
  CHECK(parse_bytes(type::real).is_none());
  CHECK(parse_bytes(type::real, 1).is_none());
  CHECK(parse_bytes(type::real, 1, 2).is_none());
  CHECK(parse_bytes(type::real, 1, 2, 3).is_none());
  CHECK(parse_bytes(type::real, 1, 2, 3, 4).is_none());
  CHECK(parse_bytes(type::real, 1, 2, 3, 4, 5).is_none());
  CHECK(parse_bytes(type::real, 1, 2, 3, 4, 5, 6, 7).is_none());
  CHECK(parse_bytes(type::real, 1, 2, 3, 4, 5, 6, 7, 8, 9).is_none());
  CHECK_EQ(parse_bytes(type::real, 64, 04, 0, 0, 0, 0, 0, 0).to_real(), 2.5);
}

TEST(parsing a string) {
  CHECK(parse_bytes(type::string).is_none());
  CHECK(parse_bytes(type::string, 3).is_none());
  CHECK(parse_bytes(type::string, 3, 'a').is_none());
  CHECK(parse_bytes(type::string, 3, 'a','b').is_none());
  CHECK(parse_bytes(type::string, 3, 'a', 'b', 'c', 'd').is_none());
  // We not only check that we get "abc", but also that the view has in fact
  // created a shallow copy while parsing.
  auto bytes = make_bytes(type::string, 3, 'a', 'b', 'c');
  auto* abc = static_cast<const void*>(bytes.data() + 2);
  if (auto val = data_view::from(std::move(bytes)); CHECK(val)) {
    auto str = val->to_string();
    CHECK_EQ(str, "abc");
    CHECK_EQ(static_cast<const void*>(str.data()), abc);
  }
}

TEST(parsing an address) {
  std::vector<uint8_t> bytes;
  bytes.push_back(static_cast<uint8_t>(type::address));
  bytes.insert(bytes.end(), localhost.bytes().begin(), localhost.bytes().end());
  auto cpy = parse_bytes_buf(bytes);
  CHECK(cpy.is_address());
  CHECK_EQ(cpy.to_address(), localhost);
}

TEST(parsing an enum value) {
  CHECK(parse_bytes(type::enum_value).is_none());
  CHECK(parse_bytes(type::enum_value, 3).is_none());
  CHECK(parse_bytes(type::enum_value, 3, 'a').is_none());
  CHECK(parse_bytes(type::enum_value, 3, 'a','b').is_none());
  CHECK(parse_bytes(type::enum_value, 3, 'a', 'b', 'c', 'd').is_none());
  // We not only check that we get "abc", but also that the view has in fact
  // created a shallow copy while parsing.
  auto bytes = make_bytes(type::enum_value, 3, 'a', 'b', 'c');
  auto* abc = static_cast<const void*>(bytes.data() + 2);
  if (auto val = data_view::from(std::move(bytes)); CHECK(val)) {
    auto str = val->to_enum_value();
    CHECK_EQ(str.name, "abc");
    CHECK_EQ(static_cast<const void*>(str.name.data()), abc);
  }
}

TEST(parsing a vector) {
  auto val = parse_bytes(type::vector, 3,                // 3-elem vector
                         type::boolean, 1,               // [0]: true
                         type::string, 3, 'a', 'b', 'c', // [1]: "abc"
                         type::vector, 2,                // [2]: 2-elem vector
                         type::integer, 0, 0, 0, 0, 0, 0, 0, 1,  // [2][0]: 1
                         type::integer, 0, 0, 0, 0, 0, 0, 0, 2); // [2][0]: 2
  if (!CHECK(val.is_vector()))
    return;
  auto xs = val.to_vector();
  if (!CHECK_EQ(xs.size(), 3))
    return;
  // [0]: true
  auto i = xs.begin();
  REQUIRE(i != xs.end());
  CHECK(i->is_boolean());
  CHECK_EQ(i->to_boolean(), true);
  // [1]: "abc"
  ++i;
  REQUIRE(i != xs.end());
  CHECK(i->is_string());
  CHECK_EQ(i->to_string(), "abc");
  // [2]: 2-elem vector
  ++i;
  REQUIRE(i != xs.end());
  CHECK(i->is_vector());
  if (auto sub_vec = i->to_vector(); CHECK_EQ(sub_vec.size(), 2u)) {
    // [2][0]: 1
    auto j = sub_vec.begin();
    REQUIRE(j != sub_vec.end());
    CHECK(j->is_integer());
    CHECK_EQ(j->to_integer(), 1);
    // [2][1]: 2
    ++j;
    REQUIRE(j != sub_vec.end());
    CHECK(j->is_integer());
    CHECK_EQ(j->to_integer(), 2);
    // end of sub vector
    ++j;
    CHECK(j == sub_vec.end());
  }
  // end of vector
  ++i;
  CHECK(i == xs.end());
}

TEST(parsing a set) {
  auto val = parse_bytes(type::set, 3,                          // 3-elem set
                         type::integer, 0, 0, 0, 0, 0, 0, 0, 1, // [0]: 1
                         type::integer, 0, 0, 0, 0, 0, 0, 0, 2, // [1]: 2
                         type::string, 3, 'a', 'b', 'c');       // [2]: "abc"
  if (!CHECK(val.is_set()))
    return;
  auto xs = val.to_set();
  if (!CHECK_EQ(xs.size(), 3))
    return;
  // [0]: 1
  auto i = xs.begin();
  REQUIRE(i != xs.end());
  CHECK(i->is_integer());
  CHECK_EQ(i->to_integer(), 1);
  // [1]: 2
  ++i;
  REQUIRE(i != xs.end());
  CHECK(i->is_integer());
  CHECK_EQ(i->to_integer(), 2);
  // [2]: "abc"
  ++i;
  REQUIRE(i != xs.end());
  CHECK(i->is_string());
  CHECK_EQ(i->to_string(), "abc");
  // end of set
  ++i;
  CHECK(i == xs.end());
}

TEST(parsing a table) {
  auto val = parse_bytes(type::table, 3,                         // 3-elem tbl
                         type::string, 4, 'k', 'e', 'y', '1',    // [0]: "key1"
                         type::integer, 0, 0, 0, 0, 0, 0, 0, 1,  // [0]: 1
                         type::string, 4, 'k', 'e', 'y', '2',    // [1]: "key2"
                         type::integer, 0, 0, 0, 0, 0, 0, 0, 2,  // [1]: 2
                         type::string, 4, 'k', 'e', 'y', '3',    // [2]: "key3"
                         type::integer, 0, 0, 0, 0, 0, 0, 0, 3); // [2]: 3
  if (!CHECK(val.is_table()))
    return;
  auto xs = val.to_table();
  if (!CHECK_EQ(xs.size(), 3))
    return;
  // Test the iterator API.
  // [0]: { "key1": 1}
  auto i = xs.begin();
  REQUIRE(i != xs.end());
  CHECK_EQ(i->first.to_string(), "key1");
  CHECK_EQ(i->second.to_integer(), 1);
  // [1]: { "key2": 2}
  ++i;
  REQUIRE(i != xs.end());
  CHECK_EQ(i->first.to_string(), "key2");
  CHECK_EQ(i->second.to_integer(), 2);
  // [2]: { "key3": 3}
  ++i;
  REQUIRE(i != xs.end());
  CHECK_EQ(i->first.to_string(), "key3");
  CHECK_EQ(i->second.to_integer(), 3);
  // end of table
  ++i;
  CHECK(i == xs.end());
  // Test the lookup API.
  CHECK_EQ(xs["key1"].to_integer(), 1);
  CHECK_EQ(xs["key2"].to_integer(), 2);
  CHECK_EQ(xs["key3"].to_integer(), 3);
  CHECK(xs["key4"].is_none());
}

TEST(data roundtrips) {
  auto to_view = [](const data& value) {
    auto buf = to_bytes(value);
    MESSAGE(value << " -> " << to_hex(buf));
    return parse_bytes_buf(buf);
  };
  auto ts = timespan{100'000};
  auto icmp = port::protocol::icmp;
  CHECK(to_view(data{}).is_none());
  CHECK_EQ(to_view(data{true}).to_boolean(), true);
  CHECK_EQ(to_view(data{42u}).to_count(), 42u);
  CHECK_EQ(to_view(data{42}).to_integer(), 42);
  CHECK_EQ(to_view(data{2.5}).to_real(), 2.5);
  CHECK_EQ(to_view(data{"foo"}).to_string(), "foo");
  CHECK_EQ(to_view(data{localhost}).to_address(), localhost);
  CHECK_EQ(to_view(data{localnet}).to_subnet(), localnet);
  CHECK_EQ(to_view(data{port{123, icmp}}).to_port(), port(123, icmp));
  CHECK_EQ(to_view(data{timestamp{ts}}).to_timestamp(), timestamp{ts});
  CHECK_EQ(to_view(data{ts}).to_timespan(), ts);
  CHECK_EQ(to_view(data{vector{1, 2, 3}}), vector({1, 2, 3}));
  CHECK_EQ(to_view(data{set{1, 2, 3}}), set({1, 2, 3}));
  CHECK_EQ(to_view(data{table{{"a", 1}, {"b", 2}, {"c", 3}}}),
           table({{"a", 1}, {"b", 2}, {"c", 3}}));
}

CAF_TEST_FIXTURE_SCOPE_END()
