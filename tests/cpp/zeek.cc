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
  CHECK_EQUAL(ev2.args(), args);
}

TEST(event_ts) {
  zeek::Event ev("test", vector{}, broker::timestamp(12s));
  REQUIRE(ev.valid());
  CHECK_EQUAL(ev.ts(), broker::timestamp(12s));
}

TEST(event_ts_metadata) {
  auto ts_md = vector{static_cast<count>(zeek::MetadataType::NetworkTimestamp),
                      broker::timestamp(12s)};
  zeek::Event ev("test", vector{}, vector{{ts_md}});
  REQUIRE(ev.valid());
  CHECK_EQUAL(ev.ts(), broker::timestamp(12s));
  CHECK_EQUAL(*get_if<broker::timestamp>(
                ev.metadata().value(zeek::MetadataType::NetworkTimestamp)),
              broker::timestamp(12s));
}

TEST(event_ts_metadata_extra) {
  auto ts_md = vector{static_cast<count>(1), broker::timestamp(12s)};
  auto extra_md = vector{static_cast<count>(4711),
                         broker::enum_value(std::string("hello"))};
  auto md = vector{ts_md, extra_md};

  zeek::Event ev("test", vector{}, md);
  REQUIRE(ev.valid());
  CHECK_EQUAL(ev.metadata().value(0), nullptr);
  CHECK_EQUAL(*get_if<timestamp>(
                ev.metadata().value(zeek::MetadataType::NetworkTimestamp)),
              broker::timestamp(12s));
  CHECK_EQUAL(*get_if<enum_value>(ev.metadata().value(4711)),
              broker::enum_value(std::string("hello")));
}

TEST(event_no_metadata) {
  zeek::Event ev("test", vector{});
  REQUIRE(ev.valid());
  CHECK_EQUAL(ev.ts(), std::nullopt);
  CHECK_EQUAL(ev.metadata().value(zeek::MetadataType::NetworkTimestamp),
              nullptr);
  CHECK_EQUAL(ev.metadata().value(1234), nullptr);
  CHECK_EQUAL(ev.metadata().get_vector(), nullptr);
  CHECK_EQUAL(ev.metadata().begin(), ev.metadata().end());
}

TEST(event_ts_bad_type) {
  auto ts_md = vector{static_cast<count>(1), broker::enum_value("invalid")};
  auto md = vector{{ts_md}};
  zeek::Event ev("test", vector{}, md);
  CHECK(!ev.valid());
}
