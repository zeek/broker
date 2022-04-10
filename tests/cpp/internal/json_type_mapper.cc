#define SUITE internal.json_type_mapper

#include "broker/internal/json_type_mapper.hh"

#include "test.hh"

#include <caf/json_reader.hpp>
#include <caf/json_writer.hpp>

using namespace broker;

using namespace std::literals;

namespace {

// A data message that has one of everything.
constexpr caf::string_view json = R"_({
  "topic": "/test/cpp/internal/json-type-mapper",
  "data": {
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
        "data": {
          "net": "2001:db8::",
          "len": 32
        }
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
      }
    ]
  }
})_";

// Gives data messages a nicer representation in JSON input and output.
struct data_message_decorator {
  topic& t;
  data& d;
};

template <class Inspector>
bool inspect(Inspector& f, data_message_decorator& x) {
  return f.object(x).fields(f.field("topic", x.t), f.field("data", x.d));
}

data_message_decorator decorated(data_message& msg) {
  auto& tup = msg.unshared();
  return data_message_decorator{get<0>(tup), get<1>(tup)};
}

timestamp timestamp_from_string(std::string ts) {
  auto opt = caf::timestamp_from_string(ts);
  if (!opt)
    FAIL("unable to parse timestamp " << ts << ": " << opt.error());
  return *opt;
}

// The same data message as above, but as native broker::data_message.
data_message native() {
  address dummy_addr;
  convert("2001:db8::"s, dummy_addr);
  vector xs;
  xs.emplace_back(nil);
  xs.emplace_back(true);
  xs.emplace_back(count{42u});
  xs.emplace_back(integer{23});
  xs.emplace_back(12.48);
  xs.emplace_back("this is a string"s);
  xs.emplace_back(dummy_addr);
  xs.emplace_back(subnet{dummy_addr, 32});
  xs.emplace_back(port{8080, port::protocol::tcp});
  xs.emplace_back(timestamp_from_string("2022-04-10T16:07:00.000"));
  xs.emplace_back(timespan{23s});
  xs.emplace_back(enum_value{"foo"s});
  xs.emplace_back(set{data{1}, data{2}, data{3}});
  table john_doe;
  // FIXME: JSON output for tables is utterly broken
  // john_doe["first-name"s] = "John"s;
  // john_doe["last-name"s] = "Doe"s;
  // xs.emplace_back(std::move(john_doe));
  return data_message{topic{"/test/cpp/internal/json-type-mapper"},
                      data{std::move(xs)}};
}

} // namespace <anonymous>

TEST(the JSON mapper enables custom type names in JSON input) {
  internal::json_type_mapper mapper;
  caf::json_reader reader;
  reader.mapper(&mapper);
  if (CHECK(reader.load(json))) {
    auto msg = data_message{};
    auto decorated_msg = decorated(msg);
    if (CHECK(reader.apply(decorated_msg)))
      CHECK_EQ(msg, native());
    else
      MESSAGE("reader reported error: " << reader.get_error());
  } else {
    MESSAGE("reader reported error: " << reader.get_error());
  }
}

TEST(the JSON mapper enables custom type names in JSON output) {
  internal::json_type_mapper mapper;
  caf::json_writer writer;
  writer.skip_object_type_annotation(true);
  writer.indentation(2);
  writer.mapper(&mapper);
  auto msg = native();
  auto decorated_msg = decorated(msg);
  if (CHECK(writer.apply(decorated_msg)))
    CHECK_EQ(writer.str(), json);
  else
    MESSAGE("writer reported error: " << writer.get_error());
}
