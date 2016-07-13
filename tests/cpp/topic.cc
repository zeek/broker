#include "broker/topic.hh"

#define SUITE topic
#include "test.hpp"

using namespace broker;

namespace {

auto sep = std::string{topic::sep};

} // namespace <anonymous>

TEST(cleaning) {
  auto sep3 = sep + sep + sep;
  CHECK_EQUAL(topic{sep3}, ""_t);
  auto t = topic{"foo" + sep3};
  CHECK_EQUAL(t, "foo"_t);
  t = sep3 + "foo";
  CHECK_EQUAL(t, sep + "foo");
  t = sep3 + "foo" + sep3;
  CHECK_EQUAL(t, sep + "foo");
}

TEST(concatenation) {
  topic t;
  t /= "foo";
  CHECK_EQUAL(t, "foo"_t);
  t /= "bar";
  CHECK_EQUAL(t, "foo" + sep + "bar");
  t /= "/baz";
  CHECK_EQUAL(t, "foo" + sep + "bar" + sep + "baz");
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
  CHECK_EQUAL(t, sep + "foo" + sep + "bar" + sep + "baz");
}
