#include "broker/data_envelope.hh"

#include "broker/broker-test.test.hh"

using namespace broker;
using namespace std::literals;

namespace {

// A data message that has one of everything.
constexpr std::string_view json = R"_({
  "type": "data-message",
  "topic": "/foo/bar",
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

} // namespace

TEST(JSON can be deserialized to a data message) {
  auto maybe_envelope = envelope::deserialize_json(json.data(), json.size());
  REQUIRE(maybe_envelope);
}

TEST(an invalid JSON payload results in an error) {
  auto no_json = "this is not json!"sv;
  auto maybe_envelope = envelope::deserialize_json(no_json.data(),
                                                   no_json.size());
  CHECK(!maybe_envelope);
}

TEST(a JSON payload that does not contain a broker data results in an error) {
  std::string_view obj = R"_({"foo": "bar"})_";
  auto maybe_envelope = envelope::deserialize_json(obj.data(), obj.size());
  CHECK(!maybe_envelope);
}
