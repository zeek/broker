#define SUITE store_event

#include "broker/store_event.hh"

#include "test.hh"

#include <string>

using namespace broker;
using namespace std::string_literals;

namespace {

struct fixture {

};

} // namespace

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
  MESSAGE("a timespan as fourth element denotes the expiry");
  {
    data x{vector{"insert"s, "foo"s, "bar"s, timespan{500}}};
    auto view = store_event::insert::make(x);
    REQUIRE(view);
    CHECK_EQUAL(view.key(), "foo"s);
    CHECK_EQUAL(view.value(), "bar"s);
    CHECK_EQUAL(view.expiry(), timespan{500});
  }
  MESSAGE("nil as fourth element is interpreted as no expiry");
  {
    data x{vector{"insert"s, "foo"s, "bar"s, nil}};
    auto view = store_event::insert::make(x);
    REQUIRE(view);
    CHECK_EQUAL(view.key(), "foo"s);
    CHECK_EQUAL(view.value(), "bar"s);
    CHECK_EQUAL(view.expiry(), nil);
  }
  MESSAGE("make returns an invalid view for malformed data");
  {
    CHECK(!store_event::insert::make(vector{"update"s, "foo"s, "bar"s, nil}));
    CHECK(!store_event::insert::make(vector{"insert"s, "foo"s, "bar"s, 42}));
    CHECK(!store_event::insert::make(vector{"insert"s, "foo"s, "bar"s}));
  }
}

TEST(update events consist of key value and expiry) {
  MESSAGE("a timespan as fifth element denotes the expiry");
  {
    data x{vector{"update"s, "foo"s, "bar"s, "baz"s, timespan{500}}};
    auto view = store_event::update::make(x);
    REQUIRE(view);
    CHECK_EQUAL(view.key(), "foo"s);
    CHECK_EQUAL(view.old_value(), "bar"s);
    CHECK_EQUAL(view.new_value(), "baz"s);
    CHECK_EQUAL(view.expiry(), timespan{500});
  }
  MESSAGE("nil as fifth element is interpreted as no expiry");
  {
    data x{vector{"update"s, "foo"s, "bar"s, "baz"s, nil}};
    auto view = store_event::update::make(x);
    REQUIRE(view);
    CHECK_EQUAL(view.key(), "foo"s);
    CHECK_EQUAL(view.old_value(), "bar"s);
    CHECK_EQUAL(view.new_value(), "baz"s);
    CHECK_EQUAL(view.expiry(), nil);
  }
  MESSAGE("make returns an invalid view for malformed data");
  {
    CHECK(!store_event::update::make(vector{"insert"s, "foo"s, "bar"s, nil}));
    CHECK(!store_event::update::make(vector{"update"s, "foo"s, "bar"s, 42}));
    CHECK(!store_event::update::make(vector{"update"s, "foo"s, "bar"s}));
  }
}

TEST(erase events consist of a key only) {
  MESSAGE("the key can have any value");
  {
    data x{vector{"erase"s, "foo"s}};
    auto view = store_event::erase::make(x);
    REQUIRE(view);
    CHECK_EQUAL(view.key(), "foo"s);
  }
  MESSAGE("make returns an invalid view for malformed data");
  {
    CHECK(!store_event::erase::make(vector{"insert"s, "foo"s}));
    CHECK(!store_event::erase::make(vector{"erase"s, "foo"s, "bar"s}));
  }
}

FIXTURE_SCOPE_END()
