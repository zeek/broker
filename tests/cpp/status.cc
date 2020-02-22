#define SUITE status

#include "broker/status.hh"

#include "test.hh"

#include <string>

using namespace broker;
using namespace std::string_literals;

TEST(status is convertible to and from string) {
  CHECK_EQUAL(to_string(sc::unspecified), "unspecified"s);
  CHECK_EQUAL(to_string(sc::peer_added), "peer_added"s);
  CHECK_EQUAL(to_string(sc::peer_removed), "peer_removed"s);
  CHECK_EQUAL(to_string(sc::peer_lost), "peer_lost"s);
  CHECK_EQUAL(sc_from_string("unspecified"), sc::unspecified);
  CHECK_EQUAL(sc_from_string("peer_added"), sc::peer_added);
  CHECK_EQUAL(sc_from_string("peer_removed"), sc::peer_removed);
  CHECK_EQUAL(sc_from_string("peer_lost"), sc::peer_lost);
  CHECK_EQUAL(sc_from_string("foo"), nil);
}
