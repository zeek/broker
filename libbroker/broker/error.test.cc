#include "broker/error.hh"

#include "broker/broker-test.test.hh"

#include "broker/internal/native.hh"

#include <string>

using namespace broker;
using namespace std::literals;

namespace {

data make_data_error(ec code, vector context = {}) {
  vector result{"error"s, enum_value{to_string(code)}, nil};
  if (!context.empty())
    result[2] = std::move(context);
  return data{std::move(result)};
}

} // namespace

FIXTURE_SCOPE(status_tests, ids_fixture)

TEST(ec is convertible to and from string) {
  auto ec_from_string = [](std::string_view str) -> std::optional<ec> {
    auto result = ec::none;
    if (convert(str, result))
      return result;
    return {};
  };
  CHECK_EQUAL(to_string(ec::unspecified), "unspecified"s);
  CHECK_EQUAL(to_string(ec::peer_incompatible), "peer_incompatible"s);
  CHECK_EQUAL(to_string(ec::peer_invalid), "peer_invalid"s);
  CHECK_EQUAL(to_string(ec::peer_unavailable), "peer_unavailable"s);
  CHECK_EQUAL(to_string(ec::peer_timeout), "peer_timeout"s);
  CHECK_EQUAL(to_string(ec::master_exists), "master_exists"s);
  CHECK_EQUAL(to_string(ec::no_such_master), "no_such_master"s);
  CHECK_EQUAL(to_string(ec::no_such_key), "no_such_key"s);
  CHECK_EQUAL(to_string(ec::request_timeout), "request_timeout"s);
  CHECK_EQUAL(to_string(ec::type_clash), "type_clash"s);
  CHECK_EQUAL(to_string(ec::invalid_data), "invalid_data"s);
  CHECK_EQUAL(to_string(ec::backend_failure), "backend_failure"s);
  CHECK_EQUAL(to_string(ec::stale_data), "stale_data"s);
  CHECK_EQUAL(to_string(ec::cannot_open_file), "cannot_open_file"s);
  CHECK_EQUAL(to_string(ec::cannot_write_file), "cannot_write_file"s);
  CHECK_EQUAL(to_string(ec::invalid_topic_key), "invalid_topic_key"s);
  CHECK_EQUAL(to_string(ec::end_of_file), "end_of_file"s);
  CHECK_EQUAL(to_string(ec::invalid_tag), "invalid_tag"s);
  CHECK_EQUAL(ec_from_string("unspecified"), ec::unspecified);
  CHECK_EQUAL(ec_from_string("peer_incompatible"), ec::peer_incompatible);
  CHECK_EQUAL(ec_from_string("peer_invalid"), ec::peer_invalid);
  CHECK_EQUAL(ec_from_string("peer_unavailable"), ec::peer_unavailable);
  CHECK_EQUAL(ec_from_string("peer_timeout"), ec::peer_timeout);
  CHECK_EQUAL(ec_from_string("master_exists"), ec::master_exists);
  CHECK_EQUAL(ec_from_string("no_such_master"), ec::no_such_master);
  CHECK_EQUAL(ec_from_string("no_such_key"), ec::no_such_key);
  CHECK_EQUAL(ec_from_string("request_timeout"), ec::request_timeout);
  CHECK_EQUAL(ec_from_string("type_clash"), ec::type_clash);
  CHECK_EQUAL(ec_from_string("invalid_data"), ec::invalid_data);
  CHECK_EQUAL(ec_from_string("backend_failure"), ec::backend_failure);
  CHECK_EQUAL(ec_from_string("stale_data"), ec::stale_data);
  CHECK_EQUAL(ec_from_string("cannot_open_file"), ec::cannot_open_file);
  CHECK_EQUAL(ec_from_string("cannot_write_file"), ec::cannot_write_file);
  CHECK_EQUAL(ec_from_string("invalid_topic_key"), ec::invalid_topic_key);
  CHECK_EQUAL(ec_from_string("end_of_file"), ec::end_of_file);
  CHECK_EQUAL(ec_from_string("invalid_tag"), ec::invalid_tag);
  CHECK_EQUAL(ec_from_string("none"), ec::none);
  CHECK_EQUAL(ec_from_string("foo"), std::nullopt);
}

TEST(default constructed errors have a fixed representation) {
  CHECK_EQUAL(get_as<data>(error{}),
              vector({"error"s, enum_value{"none"}, nil}));
  CHECK_EQUAL(get_as<error>(data{vector{"error"s, enum_value{"none"}, nil}}),
              error{});
}

TEST(errors with category broker are convertible to and from data) {
  CHECK_EQUAL(get_as<data>(make_error(ec::no_such_key)),
              make_data_error(ec::no_such_key));
  CHECK_EQUAL(get_as<error>(make_data_error(ec::no_such_key)),
              make_error(ec::no_such_key));
  CHECK_EQUAL(get_as<data>(make_error(ec::no_such_key, "my-key"s)),
              make_data_error(ec::no_such_key, {"my-key"s}));
  CHECK_EQUAL(get_as<error>(make_data_error(ec::no_such_key, {"my-key"s})),
              make_error(ec::no_such_key, "my-key"s));
  CHECK_EQUAL(
    get_as<data>(make_error(
      ec::peer_invalid,
      endpoint_info{ids['B'], network_info{"foo", 8080, timeout::seconds{42}}},
      "invalid host"s)),
    make_data_error(ec::peer_invalid,
                    {vector{str_ids['B'], "foo"s,
                            port{8080, port::protocol::tcp}, count{42}},
                     "invalid host"s}));
  CHECK_EQUAL(
    get_as<error>(make_data_error(
      ec::peer_invalid,
      {vector{str_ids['B'], "foo"s, port{8080, port::protocol::tcp}, count{42}},
       "invalid host"s})),
    make_error(ec::peer_invalid,
               endpoint_info{ids['B'],
                             network_info{"foo", 8080, timeout::seconds{42}}},
               "invalid host"s));
  CHECK_EQUAL(
    get_as<error>(make_data_error(
      ec::peer_invalid,
      {vector{nil, "foo"s, port{8080, port::protocol::tcp}, count{42}},
       "no such peer"s})),
    make_error(ec::peer_invalid,
               endpoint_info{endpoint_id{},
                             network_info{"foo", 8080, timeout::seconds{42}}},
               "invalid host"s));
}

TEST(error view operate directly on raw data) {
  data raw{vector{"error"s, enum_value{"peer_invalid"},
                  vector{vector{str_ids['B'], "foo"s,
                                port{8080, port::protocol::tcp}, count{42}},
                         "invalid host"s}}};
  auto view = make_error_view(raw);
  REQUIRE(view.valid());
  CHECK_EQUAL(view.code(), ec::peer_invalid);
  CHECK_EQUAL(*view.message(), "invalid host"s);
  auto maybe_cxt = view.context();
  REQUIRE(maybe_cxt);
  auto cxt = std::move(*maybe_cxt);
  CHECK_EQUAL(cxt.node, ids['B']);
  REQUIRE(cxt.network);
  auto net = *cxt.network;
  CHECK_EQUAL(net, network_info("foo", 8080, timeout::seconds{42}));
}

FIXTURE_SCOPE_END()
