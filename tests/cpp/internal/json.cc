#define SUITE internal.json

#include "broker/internal/json.hh"

#include "test.hh"

#include <caf/json_object.hpp>
#include <caf/json_value.hpp>
#include <caf/json_writer.hpp>

using namespace broker;

namespace {

// A data message that has one of everything.
constexpr caf::string_view json = R"_({
  "type": "data-message",
  "topic": "/test/cpp/internal/json-type-mapper",
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
      "data": "2022-04-10T16:07:00.000"
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

// The same as above, but as native broker::data_message.
data_message native() {
  using namespace std::literals;
  auto timestamp_from_string = [](std::string ts) {
    auto opt = caf::timestamp_from_string(ts);
    if (!opt)
      FAIL("unable to parse timestamp " << ts << ": " << opt.error());
    return *opt;
  };
  address dummy_addr_v6;
  convert("2001:db8::"s, dummy_addr_v6);
  address dummy_addr_v4;
  convert("255.255.255.0"s, dummy_addr_v4);
  vector xs;
  xs.emplace_back(nil);
  xs.emplace_back(true);
  xs.emplace_back(count{42u});
  xs.emplace_back(integer{23});
  xs.emplace_back(12.48);
  xs.emplace_back("this is a string"s);
  xs.emplace_back(dummy_addr_v6);
  xs.emplace_back(subnet{dummy_addr_v4, 24});
  xs.emplace_back(port{8080, port::protocol::tcp});
  xs.emplace_back(timestamp_from_string("2022-04-10T16:07:00.000"));
  xs.emplace_back(timespan{23s});
  xs.emplace_back(enum_value{"foo"s});
  xs.emplace_back(set{data{1}, data{2}, data{3}});
  table john_doe;
  john_doe["first-name"s] = "John"s;
  john_doe["last-name"s] = "Doe"s;
  xs.emplace_back(std::move(john_doe));
  return make_data_message(topic{"/test/cpp/internal/json-type-mapper"},
                           data{std::move(xs)});
}

} // namespace

TEST(a data message in JSON can be rewritten to the binary format) {
  using util = broker::internal::json;
  auto bin = std::vector<std::byte>{};
  auto val = caf::json_value::parse_shallow(json);
  REQUIRE(val);
  auto obj = val->to_object();
  CHECK_EQ(obj.value("type").to_string(), "data-message");
  auto err = util::data_message_to_binary(obj, bin);
  CHECK(!err);
  auto maybe_msg = data_envelope::deserialize(
    endpoint_id::nil(), endpoint_id::nil(), defaults::ttl,
    caf::to_string(obj.value("topic").to_string()), bin.data(), bin.size());
  REQUIRE(maybe_msg);
  CHECK_EQ((*maybe_msg)->value().to_data(), native()->value().to_data());
}

TEST(data messages can be applied to a JSON writer) {
  using util = broker::internal::json;
  caf::json_writer writer;
  writer.skip_object_type_annotation(true);
  writer.indentation(2);
  auto msg = native();
  util::apply(msg, writer);
  CHECK_EQ(writer.str(), json);
}
