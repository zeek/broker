#define SUITE detail.iterator_range

#include "broker/detail/iterator_range.hh"

#include "test.hh"

using namespace broker::detail;

namespace {

struct fixture {
  std::vector<int> xs{1, 2, 3};

  std::vector<int> ys{3, 2, 1};
};

} // namespace

FIXTURE_SCOPE(iterator_range_tests, fixture)

TEST(iterator ranges wrap iterators) {
  CHECK_EQUAL(make_iterator_range(xs).begin(), xs.begin());
  CHECK_EQUAL(make_iterator_range(xs).end(), xs.end());
  CHECK(!make_iterator_range(xs).empty());
}

TEST(iterator ranges are comparable) {
  CHECK_EQUAL(make_iterator_range(xs), make_iterator_range(xs));
  CHECK_NOT_EQUAL(make_iterator_range(ys), make_iterator_range(xs));
  CHECK_NOT_EQUAL(make_iterator_range(xs), make_iterator_range(ys));
  CHECK_EQUAL(make_iterator_range(ys), make_iterator_range(ys));
}

FIXTURE_SCOPE_END()
