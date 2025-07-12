#include "broker/format/json.hh"

#include "broker/broker-test.test.hh"
#include "broker/variant.hh"
#include "broker/variant_list.hh"
#include "broker/variant_set.hh"
#include "broker/variant_table.hh"

using namespace broker;
using namespace std::literals;

namespace {

template <class Arg>
std::string to_v1(Arg&& arg) {
  std::string result;
  auto out = std::back_inserter(result);
  format::json::v1::encode(std::forward<Arg>(arg), out);
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

// Little helper to remove all whitespaces from a string.
void strip_whitespaces(std::string& str) {
  str.erase(std::remove_if(str.begin(), str.end(), ::isspace), str.end());
}

// Returns a `time_t` value for 2014-07-09T10:16:44, i.e., the date and time of
// the first commit to the Broker repository.
timestamp broker_genesis() {
  auto secs_since_epoch = 1'404'901'004ll;
  return timestamp{timespan{secs_since_epoch * int64_t{1'000'000'000}}};
}

} // namespace

TEST(none) {
  CHECK_EQUAL(to_v1(nil), R"_({"@data-type":"none","data":{}})_");
  CHECK_EQUAL(to_v1(data{}), R"_({"@data-type":"none","data":{}})_");
}

TEST(boolean) {
  CHECK_EQUAL(to_v1(true), R"_({"@data-type":"boolean","data":true})_");
  CHECK_EQUAL(to_v1(false), R"_({"@data-type":"boolean","data":false})_");
  CHECK_EQUAL(to_v1(data{true}), R"_({"@data-type":"boolean","data":true})_");
  CHECK_EQUAL(to_v1(data{false}), R"_({"@data-type":"boolean","data":false})_");
}

TEST(count) {
  CHECK_EQUAL(to_v1(count{0}), R"_({"@data-type":"count","data":0})_");
  CHECK_EQUAL(to_v1(count{10}), R"_({"@data-type":"count","data":10})_");
  CHECK_EQUAL(to_v1(count{100}), R"_({"@data-type":"count","data":100})_");
  CHECK_EQUAL(to_v1(data{count{0}}), R"_({"@data-type":"count","data":0})_");
  CHECK_EQUAL(to_v1(data{count{10}}), R"_({"@data-type":"count","data":10})_");
}

TEST(integer) {
  CHECK_EQUAL(to_v1(integer{0}), R"_({"@data-type":"integer","data":0})_");
  CHECK_EQUAL(to_v1(integer{10}), R"_({"@data-type":"integer","data":10})_");
  CHECK_EQUAL(to_v1(integer{100}), R"_({"@data-type":"integer","data":100})_");
  CHECK_EQUAL(to_v1(integer{-10}), R"_({"@data-type":"integer","data":-10})_");
  CHECK_EQUAL(to_v1(data{integer{0}}),
              R"_({"@data-type":"integer","data":0})_");
  CHECK_EQUAL(to_v1(data{integer{10}}),
              R"_({"@data-type":"integer","data":10})_");
  CHECK_EQUAL(to_v1(data{integer{100}}),
              R"_({"@data-type":"integer","data":100})_");
  CHECK_EQUAL(to_v1(data{integer{-10}}),
              R"_({"@data-type":"integer","data":-10})_");
}

TEST(real) {
  // The encoder will call snprintf to format the value. Since the output is
  // platform-dependent, we generate the expected output dynamically.
  auto formatted = [](const char* fmt, auto... args) {
    auto size = std::snprintf(nullptr, 0, fmt, args...);
    std::vector<char> buf;
    buf.resize(static_cast<size_t>(size) + 1); // +1 for the null terminator
    size = std::snprintf(buf.data(), size + 1, fmt, args...);
    return std::string{buf.data(), static_cast<size_t>(size)};
  };
  CHECK_EQUAL(to_v1(0.0),
              formatted(R"_({"@data-type":"real","data":%f})_", 0.0));
  CHECK_EQUAL(to_v1(-10.0),
              formatted(R"_({"@data-type":"real","data":%f})_", -10.0));
  // Pass a very large number to trigger the case where the 24-byte buffer is
  // not large enough and the encoder falls back to dynamic allocation.
  CHECK_EQUAL(to_v1(1234567890123456789012345.0),
              formatted(R"_({"@data-type":"real","data":%f})_",
                        1234567890123456789012345.0));
  CHECK_EQUAL(to_v1(data{0.0}),
              formatted(R"_({"@data-type":"real","data":%f})_", 0.0));
  CHECK_EQUAL(to_v1(data{-10.0}),
              formatted(R"_({"@data-type":"real","data":%f})_", -10.0));
  // Pass a very large number to trigger the case where the 24-byte buffer is
  // not large enough and the encoder falls back to dynamic allocation.
  CHECK_EQUAL(to_v1(data{1234567890123456789012345.0}),
              formatted(R"_({"@data-type":"real","data":%f})_",
                        1234567890123456789012345.0));
}

TEST(string) {
  // Note: putting these literals into CHECK_EQUAL confuses MSVC.
  auto foobar = "foo\n\"bar"s;
  auto foobar_res = R"_({"@data-type":"string","data":"foo\n\"bar"})_";
  CHECK_EQUAL(to_v1("foo"sv), R"_({"@data-type":"string","data":"foo"})_");
  CHECK_EQUAL(to_v1(foobar), foobar_res);
  CHECK_EQUAL(to_v1(data{"foo"s}), R"_({"@data-type":"string","data":"foo"})_");
  CHECK_EQUAL(to_v1(data{foobar}), foobar_res);
}

TEST(address) {
  CHECK_EQUAL(to_v1(addr("192.128.4.4")),
              R"_({"@data-type":"address","data":"192.128.4.4"})_");
  CHECK_EQUAL(to_v1(addr("2001:db8::")),
              R"_({"@data-type":"address","data":"2001:db8::"})_");
  CHECK_EQUAL(to_v1(data{addr("192.128.4.4")}),
              R"_({"@data-type":"address","data":"192.128.4.4"})_");
  CHECK_EQUAL(to_v1(data{addr("2001:db8::")}),
              R"_({"@data-type":"address","data":"2001:db8::"})_");
}

TEST(subnet) {
  CHECK_EQUAL(to_v1(snet("192.128.4.0/24")),
              R"_({"@data-type":"subnet","data":"192.128.4.0/24"})_");
  CHECK_EQUAL(to_v1(data{snet("192.128.4.0/24")}),
              R"_({"@data-type":"subnet","data":"192.128.4.0/24"})_");
}

TEST(port) {
  CHECK_EQUAL(to_v1(port(8080, port::protocol::tcp)),
              R"_({"@data-type":"port","data":"8080/tcp"})_");
  CHECK_EQUAL(to_v1(port(9000, port::protocol::udp)),
              R"_({"@data-type":"port","data":"9000/udp"})_");
  CHECK_EQUAL(to_v1(data{port(8080, port::protocol::tcp)}),
              R"_({"@data-type":"port","data":"8080/tcp"})_");
  CHECK_EQUAL(to_v1(data{port(9000, port::protocol::udp)}),
              R"_({"@data-type":"port","data":"9000/udp"})_");
}

TEST(timespan) {
  CHECK_EQUAL(to_v1(timespan{0}), R"_({"@data-type":"timespan","data":"0s"})_");
  CHECK_EQUAL(to_v1(timespan{1ns}),
              R"_({"@data-type":"timespan","data":"1ns"})_");
  CHECK_EQUAL(to_v1(timespan{999ns}),
              R"_({"@data-type":"timespan","data":"999ns"})_");
  CHECK_EQUAL(to_v1(timespan{1000ns}),
              R"_({"@data-type":"timespan","data":"1us"})_");
  CHECK_EQUAL(to_v1(timespan{999us}),
              R"_({"@data-type":"timespan","data":"999us"})_");
  CHECK_EQUAL(to_v1(timespan{1000us}),
              R"_({"@data-type":"timespan","data":"1ms"})_");
  CHECK_EQUAL(to_v1(timespan{999ms}),
              R"_({"@data-type":"timespan","data":"999ms"})_");
  CHECK_EQUAL(to_v1(timespan{1000ms}),
              R"_({"@data-type":"timespan","data":"1s"})_");
  CHECK_EQUAL(to_v1(data{timespan{0}}),
              R"_({"@data-type":"timespan","data":"0s"})_");
  CHECK_EQUAL(to_v1(data{timespan{1ns}}),
              R"_({"@data-type":"timespan","data":"1ns"})_");
  CHECK_EQUAL(to_v1(timespan{1us}),
              R"_({"@data-type":"timespan","data":"1us"})_");
  CHECK_EQUAL(to_v1(timespan{1ms}),
              R"_({"@data-type":"timespan","data":"1ms"})_");
  CHECK_EQUAL(to_v1(timespan{1s}),
              R"_({"@data-type":"timespan","data":"1s"})_");
}

TEST(timestamp) {
  CHECK_EQUAL(
    to_v1(broker_genesis()),
    R"_({"@data-type":"timestamp","data":"2014-07-09T10:16:44.000"})_");
}

TEST(enum_value) {
  // Note: putting these literals into CHECK_EQUAL confuses MSVC.
  auto foobar = "foo\n\"bar"s;
  auto foobar_res = R"_({"@data-type":"enum-value","data":"foo\n\"bar"})_";
  CHECK_EQUAL(to_v1(enum_value{"foo"}),
              R"_({"@data-type":"enum-value","data":"foo"})_");
  CHECK_EQUAL(to_v1(enum_value{foobar}), foobar_res);
  CHECK_EQUAL(to_v1(data{enum_value{"foo"}}),
              R"_({"@data-type":"enum-value","data":"foo"})_");
  CHECK_EQUAL(to_v1(data{enum_value{foobar}}), foobar_res);
}

TEST(vector) {
  // The baseline in pretty printing mode.
  std::string baseline = R"_({
    "@data-type": "vector",
    "data": [
      {
        "@data-type": "integer",
        "data": 1
      },
      {
        "@data-type": "count",
        "data": 2
      },
      {
        "@data-type": "string",
        "data": "three"
      }
    ]
  })_";
  // Erase all whitespaces from the baseline to match the actual output.
  baseline.erase(std::remove_if(baseline.begin(), baseline.end(), isspace),
                 baseline.end());
  // Build the vector and compare it to the baseline.
  vector xs;
  xs.emplace_back(integer{1});
  xs.emplace_back(count{2});
  xs.emplace_back("three"s);
  CHECK_EQUAL(to_v1(xs), baseline);
  CHECK_EQUAL(to_v1(data{xs}), baseline);
}

TEST(set) {
  // The baseline in pretty printing mode.
  std::string baseline = R"_({
    "@data-type": "set",
    "data": [
      {
        "@data-type": "integer",
        "data": 1
      },
      {
        "@data-type": "integer",
        "data": 2
      },
      {
        "@data-type": "integer",
        "data": 3
      }
    ]
  })_";
  // Erase all whitespaces from the baseline to match the actual output.
  baseline.erase(std::remove_if(baseline.begin(), baseline.end(), isspace),
                 baseline.end());
  // Build the vector and compare it to the baseline.
  set xs;
  xs.emplace(integer{1});
  xs.emplace(integer{2});
  xs.emplace(integer{3});
  CHECK_EQUAL(to_v1(xs), baseline);
  CHECK_EQUAL(to_v1(data{xs}), baseline);
}

TEST(table) {
  // The baseline in pretty printing mode.
  std::string baseline = R"_({
    "@data-type": "table",
    "data": [
      {
        "key": {
          "@data-type": "string",
          "data": "a-key"
        },
        "value": {
          "@data-type": "integer",
          "data": 1
        }
      },
      {
        "key": {
          "@data-type": "string",
          "data": "b-key"
        },
        "value": {
          "@data-type": "count",
          "data": 42
        }
      }
    ]
  })_";
  strip_whitespaces(baseline);
  // Build the vector and compare it to the baseline.
  table xs;
  xs.emplace("a-key"s, integer{1});
  xs.emplace("b-key"s, count{42});
  CHECK_EQUAL(to_v1(xs), baseline);
  CHECK_EQUAL(to_v1(data{xs}), baseline);
}

// Little helper to make the test below more readable.
#define SECTION(name)                                                          \
  MESSAGE(name);                                                               \
  if (true)

TEST(data_message) {
  SECTION("none") {
    std::string baseline = R"_({
      "type": "data-message",
      "topic": "/test/topic",
      "@data-type": "none",
      "data": {}
    })_";
    strip_whitespaces(baseline);
    auto msg = make_data_message("/test/topic", data{});
    CHECK_EQUAL(to_v1(msg), baseline);
  }
  SECTION("boolean") {
    std::string baseline = R"_({
      "type": "data-message",
      "topic": "/test/topic",
      "@data-type": "boolean",
      "data": true
    })_";
    strip_whitespaces(baseline);
    auto msg = make_data_message("/test/topic", data{true});
    CHECK_EQUAL(to_v1(msg), baseline);
  }
  SECTION("count") {
    std::string baseline = R"_({
      "type": "data-message",
      "topic": "/test/topic",
      "@data-type": "count",
      "data": 42
    })_";
    strip_whitespaces(baseline);
    auto msg = make_data_message("/test/topic", data{count{42}});
    CHECK_EQUAL(to_v1(msg), baseline);
  }
  SECTION("integer") {
    std::string baseline = R"_({
      "type": "data-message",
      "topic": "/test/topic",
      "@data-type": "integer",
      "data": 42
    })_";
    strip_whitespaces(baseline);
    auto msg = make_data_message("/test/topic", data{integer{42}});
    CHECK_EQUAL(to_v1(msg), baseline);
  }
  SECTION("real - small") {
    std::vector<char> buf;
    buf.resize(512);
    std::snprintf(buf.data(), buf.size(),
                  R"_({
                    "type": "data-message",
                    "topic": "/test/topic",
                    "@data-type": "real",
                    "data": %f
                  })_",
                  42.0);
    auto baseline = std::string{buf.data()};
    strip_whitespaces(baseline);
    auto msg = make_data_message("/test/topic", data{real{42.0}});
    CHECK_EQUAL(to_v1(msg), baseline);
  }
  SECTION("real - big") {
    real value = 1234567890123456789012345.0;
    std::vector<char> buf;
    buf.resize(512);
    std::snprintf(buf.data(), buf.size(),
                  R"_({
                    "type": "data-message",
                    "topic": "/test/topic",
                    "@data-type": "real",
                    "data": %f
                  })_",
                  value);
    auto baseline = std::string{buf.data()};
    strip_whitespaces(baseline);
    auto msg = make_data_message("/test/topic", data{real{value}});
    CHECK_EQUAL(to_v1(msg), baseline);
  }
  SECTION("string") {
    std::string baseline = R"_({
      "type": "data-message",
      "topic": "/test/topic",
      "@data-type": "string",
      "data": "string!"
    })_";
    strip_whitespaces(baseline);
    auto msg = make_data_message("/test/topic", data{"string!"s});
    CHECK_EQUAL(to_v1(msg), baseline);
  }
  SECTION("address") {
    std::string baseline = R"_({
      "type": "data-message",
      "topic": "/test/topic",
      "@data-type": "address",
      "data": "192.128.4.4"
    })_";
    strip_whitespaces(baseline);
    auto msg = make_data_message("/test/topic", data{addr("192.128.4.4")});
    CHECK_EQUAL(to_v1(msg), baseline);
  }
  SECTION("subnet") {
    std::string baseline = R"_({
      "type": "data-message",
      "topic": "/test/topic",
      "@data-type": "subnet",
      "data": "192.128.4.0/24"
    })_";
    strip_whitespaces(baseline);
    auto msg = make_data_message("/test/topic", data{snet("192.128.4.0/24")});
    CHECK_EQUAL(to_v1(msg), baseline);
  }
  SECTION("port") {
    std::string baseline = R"_({
      "type": "data-message",
      "topic": "/test/topic",
      "@data-type": "port",
      "data": "8080/tcp"
    })_";
    strip_whitespaces(baseline);
    auto val = port(8080, port::protocol::tcp);
    auto msg = make_data_message("/test/topic", data{val});
    CHECK_EQUAL(to_v1(msg), baseline);
  }
  SECTION("timespan: 0s") {
    std::string baseline = R"_({
      "type": "data-message",
      "topic": "/test/topic",
      "@data-type": "timespan",
      "data": "0s"
    })_";
    strip_whitespaces(baseline);
    auto msg = make_data_message("/test/topic", data{timespan{0}});
    CHECK_EQUAL(to_v1(msg), baseline);
  }
  SECTION("timespan: 1ns") {
    std::string baseline = R"_({
      "type": "data-message",
      "topic": "/test/topic",
      "@data-type": "timespan",
      "data": "1ns"
    })_";
    strip_whitespaces(baseline);
    auto msg = make_data_message("/test/topic", data{timespan{1ns}});
    CHECK_EQUAL(to_v1(msg), baseline);
  }
  SECTION("timespan: 1ms") {
    std::string baseline = R"_({
      "type": "data-message",
      "topic": "/test/topic",
      "@data-type": "timespan",
      "data": "1ms"
    })_";
    strip_whitespaces(baseline);
    auto msg = make_data_message("/test/topic", data{timespan{1ms}});
    CHECK_EQUAL(to_v1(msg), baseline);
  }
  SECTION("timespan: 1s") {
    std::string baseline = R"_({
      "type": "data-message",
      "topic": "/test/topic",
      "@data-type": "timespan",
      "data": "1s"
    })_";
    strip_whitespaces(baseline);
    auto msg = make_data_message("/test/topic", data{timespan{1s}});
    CHECK_EQUAL(to_v1(msg), baseline);
  }
  SECTION("timestamp") {
    std::string baseline = R"_({
      "type": "data-message",
      "topic": "/test/topic",
      "@data-type": "timestamp",
      "data": "2014-07-09T10:16:44.000"
    })_";
    strip_whitespaces(baseline);
    auto msg = make_data_message("/test/topic", data{broker_genesis()});
    CHECK_EQUAL(to_v1(msg), baseline);
  }
  SECTION("enum_value") {
    std::string baseline = R"_({
      "type": "data-message",
      "topic": "/test/topic",
      "@data-type": "enum-value",
      "data": "foo"
    })_";
    strip_whitespaces(baseline);
    auto msg = make_data_message("/test/topic", data{enum_value{"foo"}});
    CHECK_EQUAL(to_v1(msg), baseline);
  }
  SECTION("vector") {
    std::string baseline = R"_({
      "type": "data-message",
      "topic": "/test/topic",
      "@data-type": "vector",
      "data": [
        {
          "@data-type": "integer",
          "data": 1
        },
        {
          "@data-type": "count",
          "data": 2
        },
        {
          "@data-type": "string",
          "data": "three"
        }
      ]
    })_";
    strip_whitespaces(baseline);
    vector xs;
    xs.emplace_back(integer{1});
    xs.emplace_back(count{2});
    xs.emplace_back("three"s);
    auto msg = make_data_message("/test/topic", data{xs});
    CHECK_EQUAL(to_v1(msg), baseline);
  }
  SECTION("set") {
    std::string baseline = R"_({
      "type": "data-message",
      "topic": "/test/topic",
      "@data-type": "set",
      "data": [
        {
          "@data-type": "integer",
          "data": 1
        },
        {
          "@data-type": "integer",
          "data": 2
        },
        {
          "@data-type": "integer",
          "data": 3
        }
      ]
    })_";
    strip_whitespaces(baseline);
    set xs;
    xs.emplace(integer{1});
    xs.emplace(integer{2});
    xs.emplace(integer{3});
    auto msg = make_data_message("/test/topic", data{xs});
    CHECK_EQUAL(to_v1(msg), baseline);
  }
  SECTION("table") {
    std::string baseline = R"_({
      "type": "data-message",
      "topic": "/test/topic",
      "@data-type": "table",
      "data": [
        {
          "key": {
            "@data-type": "string",
            "data": "a-key"
          },
          "value": {
            "@data-type": "integer",
            "data": 1
          }
        },
        {
          "key": {
            "@data-type": "string",
            "data": "b-key"
          },
          "value": {
            "@data-type": "count",
            "data": 42
          }
        }
      ]
    })_";
    strip_whitespaces(baseline);
    table xs;
    xs.emplace("a-key"s, integer{1});
    xs.emplace("b-key"s, count{42});
    auto msg = make_data_message("/test/topic", data{xs});
    CHECK_EQUAL(to_v1(msg), baseline);
  }
}

TEST(decode JSON into a variant) {
  constexpr std::string_view json = R"_({
    "@data-type": "vector",
    "data": [
      {
        "@data-type": "none",
        "data": {}
      },
      {
        "@data-type": "boolean",
        "data": true
      },
      {
        "@data-type": "count",
        "data": 42
      },
      {
        "@data-type": "integer",
        "data": 23
      },
      {
        "@data-type": "real",
        "data": 12.48
      },
      {
        "@data-type": "string",
        "data": "this is a string"
      },
      {
        "@data-type": "address",
        "data": "2001:db8::"
      },
      {
        "@data-type": "subnet",
        "data": "255.255.255.0/24"
      },
      {
        "@data-type": "port",
        "data": "8080/tcp"
      },
      {
        "@data-type": "timestamp",
        "data": "2014-07-09T10:16:44.000"
      },
      {
        "@data-type": "timespan",
        "data": "23s"
      },
      {
        "@data-type": "enum-value",
        "data": "foo"
      },
      {
        "@data-type": "set",
        "data": [
          {
            "@data-type": "integer",
            "data": 1
          },
          {
            "@data-type": "integer",
            "data": 2
          },
          {
            "@data-type": "integer",
            "data": 3
          }
        ]
      },
      {
        "@data-type": "table",
        "data": [
          {
            "key": {
              "@data-type": "string",
              "data": "first-name"
            },
            "value": {
              "@data-type": "string",
              "data": "John"
            }
          },
          {
            "key": {
              "@data-type": "string",
              "data": "last-name"
            },
            "value": {
              "@data-type": "string",
              "data": "Doe"
            }
          }
        ]
      }
    ]
  })_";
  variant res;
  auto err = format::json::v1::decode(json, res);
  REQUIRE(!err);
  auto xs = res.to_list();
  REQUIRE_EQ(xs.size(), 14u);
  CHECK(xs.at(0).is_none());
  CHECK_EQ(xs.at(1).to_boolean(), true);
  CHECK_EQ(xs.at(2).to_count(), 42u);
  CHECK_EQ(xs.at(3).to_integer(), 23);
  CHECK_EQ(xs.at(4).to_real(), 12.48);
  CHECK_EQ(xs.at(5).to_string(), "this is a string"sv);
  CHECK_EQ(xs.at(6).to_address(), addr("2001:db8::"));
  CHECK_EQ(xs.at(7).to_subnet(), snet("255.255.255.0/24"));
  CHECK_EQ(xs.at(8).to_port(), port(8080, port::protocol::tcp));
  CHECK_EQ(xs.at(9).to_timestamp(), broker_genesis());
  CHECK_EQ(xs.at(10).to_timespan(), timespan{23s});
  CHECK_EQ(xs.at(11).to_enum_value(), enum_value{"foo"});
  auto set = xs.at(12).to_set();
  REQUIRE_EQ(set.size(), 3u);
  CHECK(!set.contains(integer{0}));
  CHECK(set.contains(integer{1}));
  CHECK(set.contains(integer{2}));
  CHECK(set.contains(integer{3}));
  CHECK(!set.contains(integer{4}));
  auto tbl = xs.at(13).to_table();
  REQUIRE_EQ(tbl.size(), 2u);
  CHECK_EQ(tbl["first-name"].to_string(), "John"sv);
  CHECK_EQ(tbl["last-name"].to_string(), "Doe"sv);
}
