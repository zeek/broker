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
  REQUIRE(ev2.valid());
  CHECK_EQUAL(ev2.name(), "test");
  CHECK_EQUAL(ev2.ts(), std::nullopt);
  CHECK_EQUAL(ev2.args(), args);
}

TEST(event_ts) {
  auto args = vector{1, "s", port(42, port::protocol::tcp)};
  zeek::Event ev("test", vector(args), broker::timestamp(12s));
  zeek::Event ev2(std::move(ev));
  REQUIRE(ev2.valid());
  CHECK_EQUAL(ev2.name(), "test");
  CHECK_EQUAL(ev2.ts(), broker::timestamp(12s));
  CHECK_EQUAL(ev2.args(), args);
}

TEST(event_ts_metadata) {
  auto args = vector{1, "s", port(42, port::protocol::tcp)};
  auto ts_md = vector{static_cast<count>(zeek::Metadata::NETWORK_TIMESTAMP),
                      broker::timestamp(12s)};
  zeek::Event ev("test", vector(args), vector{{ts_md}});
  zeek::Event ev2(std::move(ev));
  REQUIRE(ev2.valid());
  CHECK_EQUAL(ev2.name(), "test");
  CHECK_EQUAL(ev2.ts(), broker::timestamp(12s));
  CHECK_EQUAL(ev2.args(), args);
}

TEST(event_ts_metadata_extra) {
  auto args = vector{1, "s", port(42, port::protocol::tcp)};
  auto ts_md = vector{static_cast<count>(1), broker::timestamp(12s)};
  auto extra_md = vector{static_cast<count>(4711),
                         broker::enum_value(std::string("hello"))};
  auto md = vector{ts_md, extra_md};

  zeek::Event ev("test", vector(args), md);
  zeek::Event ev2(std::move(ev));
  REQUIRE(ev2.valid());
  CHECK_EQUAL(ev2.metadata(0), nullptr);
  CHECK_EQUAL(*get_if<timestamp>(
                ev2.metadata(zeek::Metadata::NETWORK_TIMESTAMP)),
              broker::timestamp(12s));
  CHECK_EQUAL(*get_if<enum_value>(ev2.metadata(4711)),
              broker::enum_value(std::string("hello")));
}

TEST(event_no_metadata) {
  auto args = vector{1, "s", port(42, port::protocol::tcp)};
  zeek::Event ev("test", vector(args));
  zeek::Event ev2(std::move(ev));
  REQUIRE(ev2.valid());
  REQUIRE(!ev2.metadata());
  REQUIRE(!ev2.metadata(1234));
}

TEST(event_ts_bad_type) {
  auto args = vector{1, "s", port(42, port::protocol::tcp)};
  auto ts_md = vector{static_cast<count>(1), broker::enum_value("invalid")};
  auto md = vector{{ts_md}};
  zeek::Event ev("test", vector(args), md);
  REQUIRE(!ev.valid());
}
