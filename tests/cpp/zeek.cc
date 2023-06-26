// We leave testing the actual communication with Zeek to Python tests. Here we
// just check that the messages are constructed and parsed correctly.

#define SUITE zeek

#include "broker/zeek.hh"

#include "test.hh"

#include <utility>

#include "broker/data.hh"
#include "broker/time.hh"

using namespace broker;
using namespace std::literals;

using broker::zeek::ArgsBuilder;
using broker::zeek::Event;
using broker::zeek::MetadataBuilder;

TEST(event) {
  auto ev = Event::make("test", 1, "s", port(42, port::protocol::tcp));
  auto ev2 = (std::move(ev));
  auto args = ev2.args();
  REQUIRE(ev2.valid());
  CHECK(ev2.metadata().empty());
  CHECK_EQUAL(ev2.name(), "test");
  CHECK_EQUAL(args[0].to_integer(), 1);
  CHECK_EQUAL(args[1].to_string(), "s");
  CHECK_EQUAL(args[2].to_port(), port(42, port::protocol::tcp));
}

TEST(event_ts) {
  auto ev = Event::make_with_ts("test", broker::timestamp{12s}, 1, 2u);
  REQUIRE(ev.valid());
  auto args = ev.args();
  CHECK_EQUAL(args[0].to_integer(), 1);
  CHECK_EQUAL(args[1].to_count(), 2);
  CHECK_EQUAL(ev.ts(), broker::timestamp(12s));
}

TEST(event_ts_metadata) {
  auto mb = MetadataBuilder{}.add(zeek::MetadataType::NetworkTimestamp,
                                  broker::timestamp(12s));
  auto ev = Event::make_with_args("test", {}, mb);
  auto mt = ev.metadata();
  REQUIRE(ev.valid());
  CHECK(ev.args().empty());
  CHECK_EQUAL(ev.ts(), broker::timestamp(12s));
  CHECK_EQUAL(mt.value(zeek::MetadataType::NetworkTimestamp).to_timestamp(),
              broker::timestamp(12s));
}

TEST(event_ts_metadata_extra) {
  auto mb = MetadataBuilder{}
              .add(1, broker::timestamp(12s)) // NetworkTimestamp
              .add(4711u, "hello"sv);         // Custom metadata.
  auto ev = Event::make_with_args("test", {}, mb);
  auto mt = ev.metadata();
  REQUIRE(ev.valid());
  CHECK_EQUAL(ev.ts(), broker::timestamp(12s));
  CHECK_EQUAL(mt.value(1).to_timestamp(), broker::timestamp(12s));
  CHECK_EQUAL(mt.value(4711).to_string(), "hello");
}

TEST(event_ts_bad_type) {
  auto mb = MetadataBuilder{}.add(1, "invalid"sv);
  auto ev = Event::make_with_args("test", {}, mb);
  CHECK(!ev.valid());
}
