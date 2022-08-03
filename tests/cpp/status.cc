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
  // A node ID.
  endpoint_id id;

  // Output of to_string(id).
  std::string id_str;

  fixture() {
    id = endpoint_id::random(0x5EED);
    id_str = to_string(id);
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
  CHECK_EQUAL(from_string<sc>("endpoint_unreachable"),
              sc::endpoint_unreachable);
  CHECK_EQUAL(from_string<sc>("foo"), std::nullopt);
}

TEST(status is convertible to and from data) {
  CHECK_EQUAL(get_as<data>(status{}),
              vector({"status"s, enum_value{"unspecified"}, nil, nil}));
  CHECK_EQUAL(get_as<data>(status::make<sc::peer_added>(id, "text")),
              make_data_status(sc::peer_added, {id_str, nil, nil, nil},
                               "text"));
  CHECK_EQUAL(get_as<status>(make_data_status(sc::peer_added,
                                              {id_str, nil, nil, nil}, "text")),
              status::make<sc::peer_added>(id, "text"));
  CHECK_EQUAL(get_as<data>(status::make<sc::endpoint_discovered>(id, "text")),
              make_data_status(sc::endpoint_discovered, {id_str, nil, nil, nil},
                               "text"));
  CHECK_EQUAL(get_as<data>(status::make<sc::endpoint_discovered>(id, "text")),
              make_data_status(sc::endpoint_discovered, {id_str, nil, nil, nil},
                               "text"));
  CHECK_EQUAL(get_as<data>(status::make<sc::peer_added>(
                {id, network_info{"foo", 8080, timeout::seconds{42}}}, "text")),
              make_data_status(sc::peer_added,
                               {id_str, "foo"s, port{8080, port::protocol::tcp},
                                count{42}},
                               "text"));
  CHECK_EQUAL(get_as<status>(make_data_status(
                sc::peer_added,
                {id_str, "foo"s, port{8080, port::protocol::tcp}, count{42}},
                "text")),
              status::make<sc::peer_added>(
                {id, network_info{"foo", 8080, timeout::seconds{42}}}, "text"));
}

TEST(status views operate directly on raw data) {
  data raw{
    vector{"status"s, enum_value{"peer_added"},
           vector{id_str, "foo"s, port{8080, port::protocol::tcp}, count{42}},
           "text"s}};
  auto view = make_status_view(raw);
  REQUIRE(view.valid());
  CHECK_EQUAL(view.code(), sc::peer_added);
  CHECK_EQUAL(*view.message(), "text"s);
  auto cxt = view.context();
  REQUIRE(cxt);
  REQUIRE(cxt->network);
  CHECK_EQUAL(cxt->node, id);
  CHECK_EQUAL(cxt->network->address, "foo");
  CHECK_EQUAL(cxt->network->port, 8080u);
}

FIXTURE_SCOPE_END()
