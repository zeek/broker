#define SUITE filter

#include "broker/filter_type.hh"

#include "test.hh"

using namespace broker;

namespace {

struct fixture {
  template <class... Ts>
  filter_type make(Ts&&... xs) {
    filter_type result{std::forward<Ts>(xs)...};
    std::sort(result.begin(), result.end());
    return result;
  }
};

} // namespace

FIXTURE_SCOPE(filter_tests, fixture)

TEST(extending a filter with less specific topics truncates) {
  auto f = make("/foo/bar", "/foo/baz", "/zeek");
  filter_extend(f, "/foo");
  CHECK_EQUAL(f, make("/foo","/zeek"));
}

TEST(extending a filter with unrelated topics appends) {
  auto f = make("/foo/bar", "/foo/baz", "/zeek");
  filter_extend(f, "/foo/boo");
  CHECK_EQUAL(f, make("/foo/boo", "/foo/bar/", "/foo/baz", "/zeek"));
}

TEST(extending a filter with known topics does nothing) {
  auto f = make("/foo/bar", "/foo/baz", "/zeek");
  filter_extend(f, "/foo/bar");
  CHECK_EQUAL(f, make("/foo/bar/", "/foo/baz", "/zeek"));
}

FIXTURE_SCOPE_END()
