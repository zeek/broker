#define SUITE store_event

#include "broker/store_event.hh"

#include "test.hh"

#include <string>

using namespace broker;
using namespace std::string_literals;

namespace {

timespan operator""_ns(unsigned long long value) {
  return timespan{static_cast<int64_t>(value)};
}

struct fixture : base_fixture {
  uint64_t obj = 42;
};

} // namespace

#define CHECK_INVALID(kind, ...)                                               \
  do {                                                                         \
    vector tmp{__VA_ARGS__};                                                   \
    CHECK(!store_event::kind::make(tmp));                                      \
  } while (false)

FIXTURE_SCOPE(store_event_tests, fixture)

TEST(the event type is convertible to and from string) {
  CHECK_EQUAL(to_string(store_event::type::insert), "insert"s);
  CHECK_EQUAL(to_string(store_event::type::update), "update"s);
  CHECK_EQUAL(to_string(store_event::type::erase), "erase"s);
  CHECK_EQUAL(to<store_event::type>("insert"s), store_event::type::insert);
  CHECK_EQUAL(to<store_event::type>("update"s), store_event::type::update);
  CHECK_EQUAL(to<store_event::type>("erase"s), store_event::type::erase);
}

TEST(insert events consist of key value and expiry) {
  MESSAGE("a timespan as fifth element denotes the expiry");
  {
    data x{vector{"insert"s, "x"s, "foo"s, "bar"s, 500_ns, nil, nil}};
    auto view = store_event::insert::make(x);
    REQUIRE(view);
    CHECK_EQUAL(view.store_id(), "x"s);
    CHECK_EQUAL(view.key(), "foo"s);
    CHECK_EQUAL(view.value(), "bar"s);
    CHECK_EQUAL(view.expiry(), 500_ns);
    CHECK_EQUAL(view.publisher(), entity_id{});
  }
  MESSAGE("nil as fifth element is interpreted as no expiry");
  {
    data x{vector{"insert"s, "x"s, "foo"s, "bar"s, nil, nil, nil}};
    auto view = store_event::insert::make(x);
    REQUIRE(view);
    CHECK_EQUAL(view.store_id(), "x"s);
    CHECK_EQUAL(view.key(), "foo"s);
    CHECK_EQUAL(view.value(), "bar"s);
    CHECK_EQUAL(view.expiry(), std::nullopt);
    CHECK_EQUAL(view.publisher(), entity_id{});
  }
  MESSAGE("elements six and seven denote the publisher");
  {
    data x{vector{"insert"s, "x"s, "foo"s, "bar"s, nil, str_ids['B'], obj}};
    auto view = store_event::insert::make(x);
    REQUIRE(view);
    CHECK_EQUAL(view.store_id(), "x"s);
    CHECK_EQUAL(view.key(), "foo"s);
    CHECK_EQUAL(view.value(), "bar"s);
    CHECK_EQUAL(view.expiry(), std::nullopt);
    CHECK_EQUAL(view.publisher(), (entity_id{ids['B'], 42}));
  }
  MESSAGE("make returns an invalid view for malformed data");
  {
    CHECK_INVALID(insert, "update"s, "x"s, "foo"s, "bar"s, nil, nil, nil);
    CHECK_INVALID(insert, "insert"s, "x"s, "foo"s, "bar"s, 42);
    CHECK_INVALID(insert, "insert"s, "x"s, "foo"s, "bar"s);
  }
}

TEST(update events consist of key value and expiry) {
  MESSAGE("a timespan as sixth element denotes the expiry");
  {
    data x{vector{"update"s, "x"s, "foo"s, "bar"s, "baz"s, 500_ns, nil, nil}};
    auto view = store_event::update::make(x);
    REQUIRE(view);
    CHECK_EQUAL(view.store_id(), "x"s);
    CHECK_EQUAL(view.key(), "foo"s);
    CHECK_EQUAL(view.old_value(), "bar"s);
    CHECK_EQUAL(view.new_value(), "baz"s);
    CHECK_EQUAL(view.expiry(), 500_ns);
    CHECK_EQUAL(view.publisher(), entity_id{});
  }
  MESSAGE("nil as sixth element is interpreted as no expiry");
  {
    data x{vector{"update"s, "x"s, "foo"s, "bar"s, "baz"s, nil, nil, nil}};
    auto view = store_event::update::make(x);
    REQUIRE(view);
    CHECK_EQUAL(view.store_id(), "x"s);
    CHECK_EQUAL(view.key(), "foo"s);
    CHECK_EQUAL(view.old_value(), "bar"s);
    CHECK_EQUAL(view.new_value(), "baz"s);
    CHECK_EQUAL(view.expiry(), std::nullopt);
    CHECK_EQUAL(view.publisher(), entity_id{});
  }
  MESSAGE("elements six and seven denote the publisher");
  {
    data x{
      vector{"update"s, "x"s, "foo"s, "bar"s, "baz"s, nil, str_ids['B'], obj}};
    auto view = store_event::update::make(x);
    REQUIRE(view);
    CHECK_EQUAL(view.store_id(), "x"s);
    CHECK_EQUAL(view.key(), "foo"s);
    CHECK_EQUAL(view.old_value(), "bar"s);
    CHECK_EQUAL(view.new_value(), "baz"s);
    CHECK_EQUAL(view.expiry(), std::nullopt);
    CHECK_EQUAL(view.publisher(), (entity_id{ids['B'], 42}));
  }
  MESSAGE("make returns an invalid view for malformed data");
  {
    CHECK_INVALID(update, "insert"s, "x"s, "foo"s, "bar"s, nil);
    CHECK_INVALID(update, "update"s, "x"s, "foo"s, "bar"s, 42);
    CHECK_INVALID(update, "update"s, "x"s, "foo"s, "bar"s);
  }
}

TEST(erase events contain the key and optionally a publisher ID) {
  MESSAGE("the key can have any value");
  {
    data x{vector{"erase"s, "x"s, "foo"s, nil, nil}};
    auto view = store_event::erase::make(x);
    REQUIRE(view);
    CHECK_EQUAL(view.store_id(), "x"s);
    CHECK_EQUAL(view.key(), "foo"s);
    CHECK_EQUAL(view.publisher(), entity_id{});
  }
  MESSAGE("elements three and four denote the publisher");
  {
    data x{vector{"erase"s, "x"s, "foo"s, str_ids['B'], obj}};
    auto view = store_event::erase::make(x);
    REQUIRE(view);
    CHECK_EQUAL(view.store_id(), "x"s);
    CHECK_EQUAL(view.key(), "foo"s);
    CHECK_EQUAL(view.publisher(), (entity_id{ids['B'], 42}));
  }
  MESSAGE("make returns an invalid view for malformed data");
  {
    CHECK_INVALID(erase, "insert"s, "foo"s, nil, nil);
    CHECK_INVALID(erase, "erase"s, "foo"s, "bar"s, nil);
  }
}

FIXTURE_SCOPE_END()
