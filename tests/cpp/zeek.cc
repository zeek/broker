//
// We leave testing the actual communication with Zeek to Python tests. Here
// we just check that the messages are constructed and parsed correctly.
//

#include <utility>
#include "broker/zeek.hh"
#include "broker/data.hh"

#define SUITE zeek
#include "test.hpp"

using namespace broker;

TEST(event) {
  auto args = vector{1, "s", port(42, port::protocol::tcp)};
  zeek::Event ev("test", vector(args));
  zeek::Event ev2(std::move(ev));
  CHECK_EQUAL(ev2.name(), "test");
  CHECK_EQUAL(ev2.args(), args);
}
