// We leave testing the actual communication with Zeek to Python tests. Here we
// just check that the messages are constructed and parsed correctly.

#define SUITE zeek

#include "broker/zeek.hh"

#include "test.hh"

#include <utility>

#include "broker/data.hh"
#include "broker/time.hh"

using namespace broker;
using namespace std::chrono_literals;

TEST(event) {
  auto args = vector{1, "s", port(42, port::protocol::tcp)};
  zeek::Event ev("test", vector(args));
  zeek::Event ev2(std::move(ev));
  CHECK_EQUAL(ev2.name(), "test");
  CHECK_EQUAL(ev2.ts(), std::nullopt);
  CHECK_EQUAL(ev2.args(), args);
}

TEST(event_ts) {
  auto args = vector{1, "s", port(42, port::protocol::tcp)};
  zeek::Event ev("test", vector(args), broker::timestamp(12s));
  zeek::Event ev2(std::move(ev));
  CHECK_EQUAL(ev2.name(), "test");
  CHECK_EQUAL(ev2.ts(), broker::timestamp(12s));
  CHECK_EQUAL(ev2.args(), args);
}
