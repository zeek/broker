#define SUITE detail.exponential_backoff_retry_policy

#include "broker/detail/exponential_backoff_retry_policy.hh"

#include "test.hh"

using broker::detail::retry_policy_result;

using namespace broker;

namespace {

struct fixture {
  detail::exponential_backoff_retry_policy policy;
};

} // namespace

FIXTURE_SCOPE(exponential_backoff_retry_policy_tests, fixture)

TEST(the policy allows one retry by default) {
  MESSAGE("expect first timeout after two idle ticks (2^1)");
  CHECK_EQUAL(policy(1), retry_policy_result::wait);
  CHECK_EQUAL(policy(2), retry_policy_result::try_again);
  MESSAGE("expect second timeout after four more idle ticks (2^2)");
  CHECK_EQUAL(policy(1), retry_policy_result::wait);
  CHECK_EQUAL(policy(2), retry_policy_result::wait);
  CHECK_EQUAL(policy(3), retry_policy_result::wait);
  CHECK_EQUAL(policy(4), retry_policy_result::abort);
  MESSAGE("we can repeat the cycle after calling reset");
  policy.reset();
  CHECK_EQUAL(policy(1), retry_policy_result::wait);
  CHECK_EQUAL(policy(2), retry_policy_result::try_again);
  CHECK_EQUAL(policy(1), retry_policy_result::wait);
  CHECK_EQUAL(policy(2), retry_policy_result::wait);
  CHECK_EQUAL(policy(3), retry_policy_result::wait);
  CHECK_EQUAL(policy(4), retry_policy_result::abort);
}

TEST(setting a factor allows longer or shorter intervals) {
  policy.factor(0.5);
  MESSAGE("[factor 0.5] expect first timeout after two idle tick (2^0.5)");
  CHECK_EQUAL(policy(1), retry_policy_result::wait);
  CHECK_EQUAL(policy(2), retry_policy_result::try_again);
  MESSAGE("[factor 0.5] expect second timeout after two more idle ticks (2^1)");
  CHECK_EQUAL(policy(1), retry_policy_result::wait);
  CHECK_EQUAL(policy(2), retry_policy_result::abort);
  policy.reset();
  policy.factor(1.5);
  MESSAGE("[factor 1.5] expect first timeout after one idle tick (2^1.5)");
  CHECK_EQUAL(policy(1), retry_policy_result::wait);
  CHECK_EQUAL(policy(2), retry_policy_result::wait);
  CHECK_EQUAL(policy(3), retry_policy_result::try_again);
  MESSAGE("[factor 2] expect second timeout after eight more idle ticks (2^3)");
  CHECK_EQUAL(policy(1), retry_policy_result::wait);
  CHECK_EQUAL(policy(2), retry_policy_result::wait);
  CHECK_EQUAL(policy(3), retry_policy_result::wait);
  CHECK_EQUAL(policy(4), retry_policy_result::wait);
  CHECK_EQUAL(policy(5), retry_policy_result::wait);
  CHECK_EQUAL(policy(6), retry_policy_result::wait);
  CHECK_EQUAL(policy(7), retry_policy_result::wait);
  CHECK_EQUAL(policy(8), retry_policy_result::abort);
}

TEST(setting max_retries to 0 disables retries) {
  policy.max_retries(0);
  CHECK_EQUAL(policy(1), retry_policy_result::wait);
  CHECK_EQUAL(policy(2), retry_policy_result::abort);
}

FIXTURE_SCOPE_END()
