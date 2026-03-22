#pragma once

namespace broker::internal {

/// The result of invoking a callback on a handler.
enum class message_handler_pull_result {
  /// The callback was invoked successfully.
  ok,
  /// The callback was invoked but resulted in a terminal state, i.e., the
  /// handler should not be called again.
  term,
};

} // namespace broker::internal
