#define SUITE builder

#include "broker/builder.hh"

#include "test.hh"

#include <caf/detail/append_hex.hpp>

using namespace broker;
using namespace std::literals;

namespace {

std::string to_hex(std::pair<const std::byte*, const std::byte*> range) {
  auto [first, last] = range;
  std::string result;
  caf::detail::append_hex(result, first, static_cast<size_t>(last - first));
  return result;
}

std::string to_hex(data_view val){
  auto [data, size] = val.envelope_ptr()->raw_bytes();
  std::string result;
  caf::detail::append_hex(result, data, size);
  return result;
}

std::string to_hex(set_builder builder) {
  return to_hex(std::move(builder).build());
}

std::string to_hex(table_builder builder) {
  return to_hex(std::move(builder).build());
}

std::string to_hex(vector_builder builder) {
  return to_hex(std::move(builder).build());
}

struct fixture {
  address localhost;
  subnet localnet;
  timestamp tstamp;
  timespan tspan;

  fixture() {
    convert("127.0.0.1"s, localhost);
    convert("2001:db8::/32"s, localnet);
    tspan = timespan{1'000'000'000};
    tstamp = timestamp{tspan};
  }
};

} // namespace

FIXTURE_SCOPE(builder_tests, fixture)

TEST(serialize empty set) {
  set_builder builder;
  CHECK_EQUAL(builder.num_values(), 0u);
  CHECK_EQUAL(to_hex(std::move(builder)), "0C00");
}

TEST(serialize set with none) {
  set_builder builder;
  builder.add(nil);
  CHECK_EQUAL(builder.num_values(), 1u);
  CHECK_EQUAL(to_hex(builder.encoded_values()), "00");
  CHECK_EQUAL(to_hex(std::move(builder)), "0C0100");
}

TEST(serialize set with count) {
  set_builder builder;
  builder.add(1u);
  CHECK_EQUAL(builder.num_values(), 1u);
  CHECK_EQUAL(to_hex(builder.encoded_values()), "020000000000000001");
  CHECK_EQUAL(to_hex(std::move(builder)), "0C01020000000000000001");
}

TEST(serialize set with integer) {
  set_builder builder;
  builder.add(1);
  CHECK_EQUAL(builder.num_values(), 1u);
  CHECK_EQUAL(to_hex(builder.encoded_values()), "030000000000000001");
  CHECK_EQUAL(to_hex(std::move(builder)), "0C01030000000000000001");
}

TEST(serialize set with two strings) {
  set_builder builder;
  builder.add("hello"sv);
  builder.add("broker"sv);
  CHECK_EQUAL(builder.num_values(), 2u);
  CHECK_EQUAL(to_hex(builder.encoded_values()),
              "050568656C6C6F"     // "hello"
              "050662726F6B6572"); // "broker"
  CHECK_EQUAL(to_hex(std::move(builder)),
              "0C"                 // set
              "02"                 // 2 entries
              "050568656C6C6F"     // "hello"
              "050662726F6B6572"); // "broker"
}

TEST(build set with all primitive types) {
  auto val = set_builder{}
               .add(nil)
               .add(true)
               .add(24u)
               .add(42u)
               .add(-24)
               .add(-42)
               .add(2.5)
               .add("hello"sv)
               .add(localhost)
               .add(localnet)
               .add(port{80, port::protocol::tcp})
               .add(tstamp)
               .add(tspan)
               .add(enum_value{"foo"})
               .build()
               .to_set();
  MESSAGE("val: " << val);
  CHECK(val.contains(nil));
  CHECK(val.contains(true));
  CHECK(val.contains(count{24}));
  CHECK(val.contains(count{42}));
  CHECK(val.contains(integer{-24}));
  CHECK(val.contains(integer{-42}));
  CHECK(val.contains(2.5));
  CHECK(val.contains("hello"sv));
  CHECK(val.contains(localhost));
  CHECK(val.contains(localnet));
  CHECK(val.contains(port{80, port::protocol::tcp}));
  CHECK(val.contains(tstamp));
  CHECK(val.contains(tspan));
  CHECK(val.contains(enum_value_view{"foo"}));
  CHECK(!val.contains(false));
  CHECK(!val.contains(integer{42}));
}

TEST(serialize empty table) {
  table_builder builder;
  CHECK_EQUAL(builder.num_values(), 0u);
  CHECK_EQUAL(to_hex(std::move(builder)), "0D00");
}

TEST(serialize table with one entry) {
  table_builder builder;
  builder.add("k1"sv, nil);
  CHECK_EQUAL(builder.num_values(), 1u);
  CHECK_EQUAL(to_hex(builder.encoded_values()),
              "05026B31" // "k1"
              "00");     // nil
  CHECK_EQUAL(to_hex(std::move(builder)),
              "0D"       // table
              "01"       // 1 entry
              "05026B31" // "k1"
              "00");     // nil
}

TEST(serialize table with two strings) {
  table_builder builder;
  builder.add("k1"sv, "v1"sv);
  builder.add("k2"sv, "v2"sv);
  CHECK_EQUAL(builder.num_values(), 2u);
  CHECK_EQUAL(to_hex(builder.encoded_values()),
              "05026B3105027631"   // k1, v1
              "05026B3205027632"); // k2, v2
  CHECK_EQUAL(to_hex(std::move(builder)),
              "0D"                 // table
              "02"                 // 2 entries
              "05026B3105027631"   // k1, v1
              "05026B3205027632"); // k2, v2
}

TEST(serialize empty vector) {
  vector_builder builder;
  CHECK_EQUAL(builder.num_values(), 0u);
  CHECK_EQUAL(to_hex(std::move(builder)), "0E00");
}

TEST(serialize vector with none) {
  vector_builder builder;
  builder.add(nil);
  CHECK_EQUAL(builder.num_values(), 1u);
  CHECK_EQUAL(to_hex(builder.encoded_values()), "00");
  CHECK_EQUAL(to_hex(std::move(builder)), "0E0100");
}

TEST(serialize vector with two strings) {
  vector_builder builder;
  builder.add("hello"sv);
  builder.add("broker"sv);
  CHECK_EQUAL(builder.num_values(), 2u);
  CHECK_EQUAL(to_hex(builder.encoded_values()),
              "050568656C6C6F"     // "hello"
              "050662726F6B6572"); // "broker"
  CHECK_EQUAL(to_hex(std::move(builder)),
              "0E"                 // vector
              "02"                 // 2 entries
              "050568656C6C6F"     // "hello"
              "050662726F6B6572"); // "broker"
}

TEST(build vector with all primitive types) {
  auto val = vector_builder{}
               .add(nil)
               .add(true)
               .add(42u)
               .add(-42)
               .add(2.5)
               .add("hello"sv)
               .add(localhost)
               .add(localnet)
               .add(port{80, port::protocol::tcp})
               .add(tstamp)
               .add(tspan)
               .add(enum_value{"foo"})
               .build()
               .to_vector();
  MESSAGE("val: " << val);
  CHECK_EQUAL(val.size(), 12u);
  CHECK(val[0].is_none());
  CHECK_EQUAL(val[1].to_boolean(), true);
  CHECK_EQUAL(val[2].to_count(), 42u);
  CHECK_EQUAL(val[3].to_integer(), -42);
  CHECK_EQUAL(val[4].to_real(), 2.5);
  CHECK_EQUAL(val[5].to_string(), "hello"sv);
  CHECK_EQUAL(val[6].to_address(), localhost);
  CHECK_EQUAL(val[7].to_subnet(), localnet);
  CHECK_EQUAL(val[8].to_port(), port(80, port::protocol::tcp));
  CHECK_EQUAL(val[9].to_timestamp(), tstamp);
  CHECK_EQUAL(val[10].to_timespan(), tspan);
  CHECK_EQUAL(val[11].to_enum_value(), enum_value_view{"foo"});
}

CAF_TEST_FIXTURE_SCOPE_END()
