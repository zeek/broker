#define SUITE telemetry.histogram

#include "broker/telemetry/histogram.hh"

#include "test.hh"

#include "broker/telemetry/metric_registry.hh"

#include <cmath>
#include <limits>

using namespace broker;
using namespace std::literals;

namespace {

template <class Container>
auto to_vector(Container xs) {
  using value_type = std::remove_const_t<typename Container::value_type>;
  return std::vector<value_type>(xs.begin(), xs.end());
}

} // namespace

SCENARIO("telemetry managers provide access to histogram families") {
  GIVEN("a telemetry registry") {
    auto reg = telemetry::metric_registry::pre_init_instance();
    WHEN("retrieving an int_histogram family") {
      int64_t buckets[] = {10, 20};
      auto family = reg.histogram_family("broker", "payload-size", {"protocol"},
                                         buckets, "test", "bytes");
      THEN("the family object stores the parameters") {
        CHECK_EQ(family.prefix(), "broker"sv);
        CHECK_EQ(family.name(), "payload-size"sv);
        CHECK_EQ(to_vector(family.label_names()), std::vector{"protocol"s});
        CHECK_EQ(family.helptext(), "test"sv);
        CHECK_EQ(family.unit(), "bytes"sv);
        CHECK_EQ(family.is_sum(), false);
        if (CHECK_EQ(family.num_buckets(), 3u)) {
          using limits = std::numeric_limits<int64_t>;
          CHECK_EQ(family.upper_bound_at(0), 10);
          CHECK_EQ(family.upper_bound_at(1), 20);
          CHECK_EQ(family.upper_bound_at(2), limits::max());
        }
      }
      AND_THEN("get_or_add returns the same metric for the same labels") {
        auto first = family.get_or_add({{"protocol", "tcp"}});
        auto second = family.get_or_add({{"protocol", "tcp"}});
        CHECK_EQ(first, second);
      }
      AND_THEN("get_or_add returns different metric for the disjoint labels") {
        auto first = family.get_or_add({{"protocol", "tcp"}});
        auto second = family.get_or_add({{"protocol", "udp"}});
        CHECK_NE(first, second);
      }
    }
    WHEN("retrieving a dbl_histogram family") {
      double buckets[] = {10.0, 20.0};
      auto family = reg.histogram_family<double>("broker", "parse-time",
                                                 {"protocol"}, buckets, "test",
                                                 "seconds");
      THEN("the family object stores the parameters") {
        CHECK_EQ(family.prefix(), "broker"sv);
        CHECK_EQ(family.name(), "parse-time"sv);
        CHECK_EQ(to_vector(family.label_names()), std::vector{"protocol"s});
        CHECK_EQ(family.helptext(), "test"sv);
        CHECK_EQ(family.unit(), "seconds"sv);
        CHECK_EQ(family.is_sum(), false);
        if (CHECK_EQ(family.num_buckets(), 3u)) {
          CHECK_EQ(family.upper_bound_at(0), 10.0);
          CHECK_EQ(family.upper_bound_at(1), 20.0);
          CHECK(std::isinf(family.upper_bound_at(2)));
        }
      }
      AND_THEN("get_or_add returns the same metric for the same labels") {
        auto first = family.get_or_add({{"protocol", "tcp"}});
        auto second = family.get_or_add({{"protocol", "tcp"}});
        CHECK_EQ(first, second);
      }
      AND_THEN("get_or_add returns different metric for the disjoint labels") {
        auto first = family.get_or_add({{"protocol", "tcp"}});
        auto second = family.get_or_add({{"protocol", "udp"}});
        CHECK_NE(first, second);
      }
    }
  }
}
