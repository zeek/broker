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
  CHECK_EQUAL(to_string(store_event::type::add), "add"s);
  CHECK_EQUAL(to_string(store_event::type::put), "put"s);
  CHECK_EQUAL(to_string(store_event::type::erase), "erase"s);
  CHECK_EQUAL(to<store_event::type>("add"s), store_event::type::add);
  CHECK_EQUAL(to<store_event::type>("put"s), store_event::type::put);
  CHECK_EQUAL(to<store_event::type>("erase"s), store_event::type::erase);
}

TEST(add events consist of key value and expiry) {
  MESSAGE("a timespan as fourth element denotes the expiry");
  {
    data x{vector{"add"s, "foo"s, "bar"s, timespan{500}}};
    auto view = store_event::add::make(x);
    REQUIRE(view);
    CHECK_EQUAL(view.key(), "foo"s);
    CHECK_EQUAL(view.value(), "bar"s);
    CHECK_EQUAL(view.expiry(), timespan{500});
  }
  MESSAGE("nil as fourth element is interpreted as no expiry");
  {
    data x{vector{"add"s, "foo"s, "bar"s, nil}};
    auto view = store_event::add::make(x);
    REQUIRE(view);
    CHECK_EQUAL(view.key(), "foo"s);
    CHECK_EQUAL(view.value(), "bar"s);
    CHECK_EQUAL(view.expiry(), nil);
  }
  MESSAGE("make returns an invalid view for malformed data");
  {
    CHECK(!store_event::add::make(data{vector{"put"s, "foo"s, "bar"s, nil}}));
    CHECK(!store_event::add::make(data{vector{"add"s, "foo"s, "bar"s, 42}}));
    CHECK(!store_event::add::make(data{vector{"add"s, "foo"s, "bar"s}}));
  }
}

TEST(put events consist of key value and expiry) {
  MESSAGE("a timespan as fourth element denotes the expiry");
  {
    data x{vector{"put"s, "foo"s, "bar"s, timespan{500}}};
    auto view = store_event::put::make(x);
    REQUIRE(view);
    CHECK_EQUAL(view.key(), "foo"s);
    CHECK_EQUAL(view.value(), "bar"s);
    CHECK_EQUAL(view.expiry(), timespan{500});
  }
  MESSAGE("nil as fourth element is interpreted as no expiry");
  {
    data x{vector{"put"s, "foo"s, "bar"s, nil}};
    auto view = store_event::put::make(x);
    REQUIRE(view);
    CHECK_EQUAL(view.key(), "foo"s);
    CHECK_EQUAL(view.value(), "bar"s);
    CHECK_EQUAL(view.expiry(), nil);
  }
  MESSAGE("make returns an invalid view for malformed data");
  {
    CHECK(!store_event::put::make(data{vector{"add"s, "foo"s, "bar"s, nil}}));
    CHECK(!store_event::put::make(data{vector{"put"s, "foo"s, "bar"s, 42}}));
    CHECK(!store_event::put::make(data{vector{"put"s, "foo"s, "bar"s}}));
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
    CHECK(!store_event::erase::make(data{vector{"add"s, "foo"s}}));
    CHECK(!store_event::erase::make(data{vector{"erase"s, "foo"s, "bar"s}}));
  }
}

FIXTURE_SCOPE_END()
