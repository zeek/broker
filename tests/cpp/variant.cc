#include <string>

#include "broker/detail/variant.hh"

#define SUITE variant
#include "test.hpp"

using namespace broker;

// FIXME: why aren't get() and other free functions found via ADL?
//using detail::variant;
using namespace broker::detail;

using triple = variant<int, double, std::string>;

TEST(default construction) {
  triple t;
  CHECK_EQUAL(t.index(), 0u);
  REQUIRE(get_if<int>(t));
  CHECK_EQUAL(get<int>(t), 0);
}

TEST(type construction) {
  auto t = triple{"42"};
  CHECK_EQUAL(t.index(), 2u);
  REQUIRE(get_if<std::string>(t));
  CHECK_EQUAL(get<std::string>(t), "42");
}

TEST(assignment) {
  auto t0 = triple{42};
  auto t1 = triple{4.2};
  auto t2 = triple{"42"};
  CHECK_EQUAL(t0.index(), 0u);
  CHECK_EQUAL(t1.index(), 1u);
  CHECK_EQUAL(t2.index(), 2u);
  get<int>(t0) = 1337;
  get<double>(t1) = 1.337;
  get<std::string>(t2) = "1337";
  CHECK_EQUAL(get<int>(t0), 1337);
  CHECK_EQUAL(get<double>(t1), 1.337);
  CHECK_EQUAL(get<std::string>(t2), "1337");
}

TEST(relational operators) {
  using pair = variant<double, int>;
  pair p0{42};
  pair p1{42.0};
  pair p2{1337};
  pair p3{4.2};

  MESSAGE("equality");
  CHECK(p0 != p1);
  CHECK(p0 != p2);
  CHECK(p0 != p3);
  CHECK(p1 != p3);
  p1 = 4.2;
  CHECK(p1 == p3);

  MESSAGE("total order");
  CHECK(!(p1 < p3 || p1 > p3));
  CHECK(p1 < p2);
  CHECK(p2 > p1);
  CHECK(p0 < p2);
  // double types are less than int types within the variant ordering.
  CHECK(p1 < p0);
  CHECK(p1 < p2);
  CHECK(p3 < p2);
}

struct stateful {
  using result_type = void;

  template <class T>
  result_type operator()(T&) {
    ++state;
  }

  int state = 0;
};

struct doppler {
  using result_type = void;

  template <class T>
  result_type operator()(T& x) const {
    x += x;
  }
};

TEST(unary visitation) {
  auto t = triple{42};
  stateful s;
  visit(s, t);         // lvalue
  visit(s, t);         // lvalue
  CHECK_EQUAL(s.state, 2);
  visit(doppler{}, t); // rvalue
  CHECK_EQUAL(get<int>(t), 42 * 2);
}

struct binary {
  using result_type = bool;

  template <typename T>
  result_type operator()(const T&, const T&) const {
    return true;
  }

  template <typename T, typename U>
  result_type operator()(const T&, const U&) const {
    return false;
  }
};

TEST(binary visitation) {
  auto t0 = triple{42};
  auto t1 = triple{4.2};
  auto t2 = triple{"42"};
  CHECK(!visit(binary{}, t0, t1));
  CHECK(!visit(binary{}, t1, t0));
  CHECK(!visit(binary{}, t0, t2));
  CHECK(visit(binary{}, t0, triple{84}));
}

struct ternary {
  using result_type = double;

  template <typename T, typename U>
  result_type operator()(bool c, T const& t, U const& f) const {
    return static_cast<double>(c ? t : f);
  }

  template <typename T, typename U, typename V>
  result_type operator()(T const&, U const&, V const&) const {
    return 4.2;
  }
};

TEST(ternary visitation) {
  using trio = variant<bool, double, int>;
  CHECK_EQUAL(visit(ternary{}, trio{true}, trio{4.2}, trio{42}), 4.2);
  CHECK_EQUAL(visit(ternary{}, trio{false}, trio{4.2}, trio{1337}), 1337.0);
}

TEST(delayed visitation) {
  std::vector<variant<double, int>> doubles = {1337, 4.2, 42};
  stateful s;
  std::for_each(doubles.begin(), doubles.end(), visit(std::ref(s)));
  CHECK_EQUAL(s.state, 3);
}
