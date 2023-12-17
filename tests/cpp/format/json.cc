#define SUITE format.json

#include "broker/format/json.hh"

#include "test.hh"

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
  CHECK_EQUAL(to_v1("foo"sv), R"_({"@data-type":"string","data":"foo"})_");
  CHECK_EQUAL(to_v1("foo\nbar"sv),
              R"_({"@data-type":"string","data":"foo\nbar"})_");
  CHECK_EQUAL(to_v1("foo\n\"bar"sv),
              R"_({"@data-type":"string","data":"foo\n\"bar"})_");
  CHECK_EQUAL(to_v1(data{"foo"s}), R"_({"@data-type":"string","data":"foo"})_");
  CHECK_EQUAL(to_v1(data{"foo\nbar"s}),
              R"_({"@data-type":"string","data":"foo\nbar"})_");
  CHECK_EQUAL(to_v1(data{"foo\n\"bar"s}),
              R"_({"@data-type":"string","data":"foo\n\"bar"})_");
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
  // Jul 9, 2014, 5:16 PM: time of the first Broker commit.
  auto datetime = timestamp{timespan{140'4918'960'000'000'000}};
  CHECK_EQUAL(to_v1(datetime), R"_({"@data-type":"timestamp","data":"2014-07-09T17:16:00.000"})_");
}

TEST(enum_value) {
  CHECK_EQUAL(to_v1(enum_value{"foo"}),
              R"_({"@data-type":"enum-value","data":"foo"})_");
  CHECK_EQUAL(to_v1(enum_value{"foo\nbar"}),
              R"_({"@data-type":"enum-value","data":"foo\nbar"})_");
  CHECK_EQUAL(to_v1(enum_value{"foo\n\"bar"}),
              R"_({"@data-type":"enum-value","data":"foo\n\"bar"})_");
  CHECK_EQUAL(to_v1(data{enum_value{"foo"}}),
              R"_({"@data-type":"enum-value","data":"foo"})_");
  CHECK_EQUAL(to_v1(data{enum_value{"foo\nbar"}}),
              R"_({"@data-type":"enum-value","data":"foo\nbar"})_");
  CHECK_EQUAL(to_v1(data{enum_value{"foo\n\"bar"}}),
              R"_({"@data-type":"enum-value","data":"foo\n\"bar"})_");
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

TEST(table){
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
  // Erase all whitespaces from the baseline to match the actual output.
  baseline.erase(std::remove_if(baseline.begin(), baseline.end(), isspace),
                 baseline.end());
  // Build the vector and compare it to the baseline.
  table xs;
  xs.emplace("a-key"s, integer{1});
  xs.emplace("b-key"s, count{42});
  CHECK_EQUAL(to_v1(xs), baseline);
  CHECK_EQUAL(to_v1(data{xs}), baseline);
}
