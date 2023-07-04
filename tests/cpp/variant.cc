#define SUITE variant

#include "broker/variant.hh"

#include "test.hh"

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>

#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/variant_list.hh"
#include "broker/variant_set.hh"
#include "broker/variant_table.hh"

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

class envelope_test_impl : public envelope {
public:
  envelope_test_impl(byte_buffer bytes) : bytes_(std::move(bytes)) {}

  variant value() const noexcept override {
    return {root_, shared_from_this()};
  }

  std::string_view topic() const noexcept override {
    using namespace std::literals;
    return "test"sv;
  }

  bool is_root(const variant_data* val) const noexcept override {
    return val == root_;
  }

  std::pair<const std::byte*, size_t> raw_bytes() const noexcept override {
    return {reinterpret_cast<const std::byte*>(bytes_.data()), bytes_.size()};
  }

  error parse() {
    error result;
    root_ = do_parse(buf_, result);
    return result;
  }

private:
  variant_data* root_ = nullptr;
  caf::byte_buffer bytes_;
  detail::monotonic_buffer_resource buf_;
};

template<class T, class... Ts>
error parse_bytes_result(T arg, Ts... args) {
  if constexpr (sizeof...(Ts) == 0 && std::is_same_v<byte_buffer, T>) {
    auto envelope = std::make_shared<envelope_test_impl>(std::move(arg));
    return envelope->parse();
  } else {
    return parse_bytes(make_bytes(arg, args...));
  }
}

template<class T, class... Ts>
variant parse_bytes(T arg, Ts... args) {
  if constexpr (sizeof...(Ts) == 0 && std::is_same_v<byte_buffer, T>) {
    auto envelope = std::make_shared<envelope_test_impl>(std::move(arg));
    if (auto err = envelope->parse(); err)
      return variant{};
    return envelope->value();
  } else {
    return parse_bytes(make_bytes(arg, args...));
  }
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

FIXTURE_SCOPE(variant_tests, fixture)

TEST(default construction) {
  CHECK(variant{}.is_none());
  CHECK_EQ(variant{}, variant{});
}

TEST(parsing a none) {
  CHECK_EQ(parse_bytes_result(make_bytes(type::none)), error{});
  CHECK_EQ(parse_bytes_result(make_bytes(type::none, 1)),
           ec::deserialization_failed);
  CHECK(parse_bytes(make_bytes(type::none)).is_none());
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
           0x0102030405060708u);
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
  auto val = parse_bytes(std::move(bytes));
  auto str = val.to_string();
  CHECK_EQ(str, "abc");
  CHECK_EQ(static_cast<const void*>(str.data()), abc);
}

TEST(parsing an address) {
  byte_buffer bytes;
  bytes.push_back(static_cast<caf::byte>(type::address));
  auto* begin = reinterpret_cast<const caf::byte*>(localhost.bytes().data());
  bytes.insert(bytes.end(), begin, begin + localhost.bytes().size());
  auto cpy = parse_bytes(bytes);
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
  auto val = parse_bytes(std::move(bytes));
  auto str = val.to_enum_value();
  CHECK_EQ(str.name, "abc");
  CHECK_EQ(static_cast<const void*>(str.name.data()), abc);
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
  if (!CHECK_EQ(xs.size(), 3u))
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
  if (!CHECK_EQ(xs.size(), 3u))
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
  if (!CHECK_EQ(xs.size(), 3u))
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

namespace {

auto to_variant(const data& value) {
  auto buf = to_bytes(value);
  MESSAGE(value << " -> " << to_hex(buf));
  return parse_bytes(buf);
}

} // namespace

TEST(data serialization roundtrips) {
  auto ts = timespan{100'000};
  auto icmp = port::protocol::icmp;
  CHECK(to_variant(data{}).is_none());
  CHECK_EQ(to_variant(data{true}).to_boolean(), true);
  CHECK_EQ(to_variant(data{42u}).to_count(), 42u);
  CHECK_EQ(to_variant(data{42}).to_integer(), 42);
  CHECK_EQ(to_variant(data{2.5}).to_real(), 2.5);
  CHECK_EQ(to_variant(data{"foo"}).to_string(), "foo");
  CHECK_EQ(to_variant(data{localhost}).to_address(), localhost);
  CHECK_EQ(to_variant(data{localnet}).to_subnet(), localnet);
  CHECK_EQ(to_variant(data{port{123, icmp}}).to_port(), port(123, icmp));
  CHECK_EQ(to_variant(data{timestamp{ts}}).to_timestamp(), timestamp{ts});
  CHECK_EQ(to_variant(data{ts}).to_timespan(), ts);
  CHECK_EQ(to_variant(data{vector{1, 2, 3}}), vector({1, 2, 3}));
  CHECK_EQ(to_variant(data{set{1, 2, 3}}), set({1, 2, 3}));
  CHECK_EQ(to_variant(data{table{{"a", 1}, {"b", 2}, {"c", 3}}}),
           table({{"a", 1}, {"b", 2}, {"c", 3}}));
}

#define CHECK_DEEP_COPY_ROUNDTRIP(data_init)                                   \
  {                                                                            \
    auto value = data{data_init};                                              \
    CHECK_EQ(to_variant(value).to_data(), value);                              \
  }                                                                            \
  static_cast<void>(0)

TEST(deep copies) {
  auto ts = timespan{100'000};
  CHECK_DEEP_COPY_ROUNDTRIP(true);
  CHECK_DEEP_COPY_ROUNDTRIP(false);
  CHECK_DEEP_COPY_ROUNDTRIP(42);
  CHECK_DEEP_COPY_ROUNDTRIP(42u);
  CHECK_DEEP_COPY_ROUNDTRIP(2.5);
  CHECK_DEEP_COPY_ROUNDTRIP("foo"s);
  CHECK_DEEP_COPY_ROUNDTRIP(localhost);
  CHECK_DEEP_COPY_ROUNDTRIP(localnet);
  CHECK_DEEP_COPY_ROUNDTRIP(port(123, port::protocol::icmp));
  CHECK_DEEP_COPY_ROUNDTRIP(timestamp{ts});
  CHECK_DEEP_COPY_ROUNDTRIP(ts);
  CHECK_DEEP_COPY_ROUNDTRIP(vector({1, 2, 3}));
  CHECK_DEEP_COPY_ROUNDTRIP(set({1, 2, 3}));
  CHECK_DEEP_COPY_ROUNDTRIP(table({{"a", 1}, {"b", 2}, {"c", 3}}));
}

CAF_TEST_FIXTURE_SCOPE_END()
