#define SUITE status

#include "broker/status.hh"

#include "test.hh"

#include <string>

using namespace broker;
using namespace std::string_literals;

namespace {

data make_data_status(sc code, vector context, std::string text) {
  vector result;
  result.resize(4);
  result[0] = "status"s;
  result[1] = enum_value{to_string(code)};
  if (!context.empty())
    result[2] = std::move(context);
  result[3] = std::move(text);
  return data{std::move(result)};
}

struct fixture {
  // A node ID in CAF's default format (host hash + process ID).
  caf::node_id def_id;

  // Output of to_string(def_id).
  std::string def_id_str;

  // A node ID in CAF's URI format (e.g., ip://foo:123).
  caf::node_id uri_id;

  // Output of to_string(uri_id).
  std::string uri_id_str;

  fixture() {
    auto id = caf::make_node_id(10, "402FA79E64ACFA54522FFC7AC886630670517900");
    if (!id)
      FAIL("caf::make_node_id failed");
    def_id = std::move(*id);
    def_id_str = to_string(def_id);
    auto tmp = caf::make_uri("ip://foo:8080");
    if (!tmp)
      FAIL("caf::make_uri failed");
    uri_id = caf::make_node_id(std::move(*tmp));
    uri_id_str = to_string(uri_id);
  }
};

} // namespace

FIXTURE_SCOPE(status_tests, fixture)

TEST(sc is convertible to and from string) {
  CHECK_EQUAL(to_string(sc::unspecified), "unspecified"s);
  CHECK_EQUAL(to_string(sc::peer_added), "peer_added"s);
  CHECK_EQUAL(to_string(sc::peer_removed), "peer_removed"s);
  CHECK_EQUAL(to_string(sc::peer_lost), "peer_lost"s);
  CHECK_EQUAL(from_string<sc>("unspecified"), sc::unspecified);
  CHECK_EQUAL(to_string(sc::endpoint_discovered), "endpoint_discovered"s);
  CHECK_EQUAL(to_string(sc::endpoint_unreachable), "endpoint_unreachable"s);
  CHECK_EQUAL(from_string<sc>("peer_added"), sc::peer_added);
  CHECK_EQUAL(from_string<sc>("peer_removed"), sc::peer_removed);
  CHECK_EQUAL(from_string<sc>("peer_lost"), sc::peer_lost);
  CHECK_EQUAL(from_string<sc>("endpoint_discovered"), sc::endpoint_discovered);
  CHECK_EQUAL(from_string<sc>("endpoint_unreachable"), sc::endpoint_unreachable);
  CHECK_EQUAL(from_string<sc>("foo"), nil);
}

TEST(status is convertible to and from data) {
  CHECK_EQUAL(get_as<data>(status{}),
              vector({"status"s, enum_value{"unspecified"}, nil, nil}));
  CHECK_EQUAL(get_as<data>(status::make<sc::peer_added>({uri_id, nil}, "text")),
              make_data_status(sc::peer_added, {uri_id_str, nil, nil, nil},
                               "text"));
  CHECK_EQUAL(get_as<status>(make_data_status(
                sc::peer_added, {uri_id_str, nil, nil, nil}, "text")),
              status::make<sc::peer_added>({uri_id, nil}, "text"));
  CHECK_EQUAL(get_as<data>(status::make<sc::peer_added>({def_id, nil}, "text")),
              make_data_status(sc::peer_added, {def_id_str, nil, nil, nil},
                               "text"));
  CHECK_EQUAL(get_as<status>(make_data_status(
                sc::peer_added, {def_id_str, nil, nil, nil}, "text")),
              status::make<sc::peer_added>({def_id, nil}, "text"));
  CHECK_EQUAL(get_as<data>(
                status::make<sc::endpoint_discovered>(def_id, "text")),
              make_data_status(sc::endpoint_discovered,
                               {def_id_str, nil, nil, nil}, "text"));
  CHECK_EQUAL(get_as<data>(
                status::make<sc::endpoint_discovered>(def_id, "text")),
              make_data_status(sc::endpoint_discovered,
                               {def_id_str, nil, nil, nil}, "text"));
  CHECK_EQUAL(get_as<data>(status::make<sc::peer_added>(
                {def_id, network_info{"foo", 8080, timeout::seconds{42}}},
                "text")),
              make_data_status(sc::peer_added,
                               {def_id_str, "foo"s,
                                port{8080, port::protocol::tcp}, count{42}},
                               "text"));
  CHECK_EQUAL(
    get_as<status>(make_data_status(
      sc::peer_added,
      {def_id_str, "foo"s, port{8080, port::protocol::tcp}, count{42}},
      "text")),
    status::make<sc::peer_added>(
      {def_id, network_info{"foo", 8080, timeout::seconds{42}}}, "text"));
}

TEST(status views operate directly on raw data) {
  data raw{vector{"status"s, enum_value{"peer_added"},
                  vector{def_id_str, "foo"s, port{8080, port::protocol::tcp},
                         count{42}},
                  "text"s}};
  auto view = make_status_view(raw);
  REQUIRE(view.valid());
  CHECK_EQUAL(view.code(), sc::peer_added);
  CHECK_EQUAL(*view.message(), "text"s);
  auto cxt = view.context();
  REQUIRE(cxt);
  REQUIRE(cxt->network);
  CHECK_EQUAL(cxt->node, def_id);
  CHECK_EQUAL(cxt->network->address, "foo");
  CHECK_EQUAL(cxt->network->port, 8080u);
}

FIXTURE_SCOPE_END()
