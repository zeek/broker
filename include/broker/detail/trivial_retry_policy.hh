#pragma once

#include "broker/detail/retry_policy_result.hh"

namespace broker::detail {

/// A trivial retry policy for a @ref channel consumer that retries every NACK
/// timeout without an upper bound for maximum retries.
class trivial_retry_policy {
public:
  constexpr retry_policy_result
  operator()(uint16_t idle_ticks, uint16_t nack_timeout) const noexcept {
    return idle_ticks < nack_timeout ? retry_policy_result::wait
                                     : retry_policy_result::try_again;
  }

  constexpr void reset() const noexcept {
    // nop
  }
};

} // namespace broker::detail
