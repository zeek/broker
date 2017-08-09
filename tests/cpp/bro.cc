//
// We leave testing the actual communication with Bro to Python tests. Here
// we just check that the messages are constructed and parsed correctly.
//

#include "broker/broker.hh"
#include "broker/bro.hh"

#define SUITE bro
#include "test.hpp"

using namespace broker;

TEST(event) {
  auto args = vector{1, "s", port(42, port::protocol::tcp)};
  bro::Event ev("test", args);
  data d = ev;
  auto hdr = vector{broker::count(1), broker::count(1)};
  CHECK_EQUAL(d, (vector{hdr, vector{"test", args}}));
  bro::Event ev2(d);
  CHECK_EQUAL(ev2.name(), "test");
  CHECK_EQUAL(ev2.args(), args);
}
