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
  CHECK_EQUAL(from_string<sc>("unspecified"), sc::unspecified);
  CHECK_EQUAL(from_string<sc>("peer_added"), sc::peer_added);
  CHECK_EQUAL(from_string<sc>("peer_removed"), sc::peer_removed);
  CHECK_EQUAL(from_string<sc>("peer_lost"), sc::peer_lost);
  CHECK_EQUAL(from_string<sc>("foo"), nil);
}
