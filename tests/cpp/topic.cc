#define SUITE topic

#include "broker/topic.hh"

#include "test.hh"

using namespace broker;

namespace {

auto sep = std::string{topic::sep};

} // namespace

TEST(concatenation) {
  topic t;
  t /= "foo";
  CHECK_EQUAL(t.string(), "foo");
  t /= "bar";
  CHECK_EQUAL(t.string(), "foo" + sep + "bar");
  t /= "/baz";
  CHECK_EQUAL(t.string(), "foo" + sep + "bar" + sep + "baz");
}

TEST(split) {
  auto xs = topic::split("foo/bar/baz"_t);
  REQUIRE_EQUAL(xs.size(), 3u);
  CHECK_EQUAL(xs[0], "foo");
  CHECK_EQUAL(xs[1], "bar");
  CHECK_EQUAL(xs[2], "baz");
  auto framed = topic::split("/foo/bar/baz/"_t);
  CHECK(xs == framed);
}

TEST(join) {
  std::vector<std::string> xs{"/foo", "bar/", "/baz"};
  auto t = topic::join(xs);
  CHECK_EQUAL(t.string(), sep + "foo" + sep + "bar" + sep + "baz");
}

TEST(prefix) {
  topic t0 = "/zeek/";
  topic t1 = "/zeek/events/";
  topic t2 = "/zeek/events/debugging/";
  topic t3 = "/zeek/stores/";
  topic t4 = "/zeek/stores/masters/";
  topic t5 = "/";
  // t0 is a prefix of all topics except t5
  CHECK(t0.prefix_of(t0));
  CHECK(t0.prefix_of(t1));
  CHECK(t0.prefix_of(t2));
  CHECK(t0.prefix_of(t3));
  CHECK(t0.prefix_of(t4));
  CHECK(!t0.prefix_of(t5));
  // t1 is a prefix of itself and t2
  CHECK(!t1.prefix_of(t0));
  CHECK(t1.prefix_of(t1));
  CHECK(t1.prefix_of(t2));
  CHECK(!t1.prefix_of(t3));
  CHECK(!t1.prefix_of(t4));
  CHECK(!t1.prefix_of(t5));
  // t2 is only a prefix of itself
  CHECK(!t2.prefix_of(t0));
  CHECK(!t2.prefix_of(t1));
  CHECK(t2.prefix_of(t2));
  CHECK(!t2.prefix_of(t3));
  CHECK(!t2.prefix_of(t4));
  CHECK(!t2.prefix_of(t5));
  // t3 is a prefix of itself and t4
  CHECK(!t3.prefix_of(t0));
  CHECK(!t3.prefix_of(t1));
  CHECK(!t3.prefix_of(t2));
  CHECK(t3.prefix_of(t3));
  CHECK(t3.prefix_of(t4));
  CHECK(!t3.prefix_of(t5));
  // t4 is only a prefix of itself
  CHECK(!t4.prefix_of(t0));
  CHECK(!t4.prefix_of(t1));
  CHECK(!t4.prefix_of(t2));
  CHECK(!t4.prefix_of(t3));
  CHECK(t4.prefix_of(t4));
  CHECK(!t4.prefix_of(t5));
  // t5 is a prefix of all topics
  CHECK(t5.prefix_of(t0));
  CHECK(t5.prefix_of(t1));
  CHECK(t5.prefix_of(t2));
  CHECK(t5.prefix_of(t3));
  CHECK(t5.prefix_of(t4));
  CHECK(t5.prefix_of(t5));
}
