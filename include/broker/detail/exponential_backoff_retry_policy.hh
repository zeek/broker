#pragma once

#include <cmath>
#include <cstddef>

#include "broker/detail/retry_policy_result.hh"

namespace broker::detail {

class exponential_backoff_retry_policy {
public:
  retry_policy_result operator()(uint16_t idle_ticks, uint16_t = 0) noexcept {
    auto threshold = std::pow(2.0, factor_ * retries_);
    if (idle_ticks < threshold)
      return retry_policy_result::wait;
    if (retries_ > max_retries_)
      return retry_policy_result::abort;
    ++retries_;
    return retry_policy_result::try_again;
  }

  void reset() noexcept {
    retries_ = 1;
  }

  void max_retries(uint16_t value) noexcept {
    max_retries_ = value;
  }

  void factor(double value) noexcept {
    factor_ = value;
  }

private:
  uint16_t retries_ = 1;
  uint16_t max_retries_ = 1;
  double factor_ = 1.0;
};

} // namespace broker::detail
