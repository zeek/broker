#pragma once

#include <string>

namespace broker::detail {

/// Wraps the result of a @ref channel retry policy.
enum class retry_policy_result {
  /// Indicates that the current tick triggered no state transition.
  wait,
  /// Indicates that a retry timeout was reached and a new attempt starts.
  try_again,
  /// Indicates that the operation timed out.
  abort,
};

/// @relates retry_policy_result
std::string to_string(retry_policy_result);

} // namespace broker::detail
