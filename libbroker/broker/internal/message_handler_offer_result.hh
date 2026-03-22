#pragma once

namespace broker::internal {

/// The result of invoking `offer` on a handler.
enum class message_handler_offer_result {
  /// The message was accepted by the handler.
  ok,
  /// The message was skipped (ignored) by the handler.
  skip,
  /// The handler hit the buffer limit and disconnected.
  overflow_disconnect,
  /// The handler shuts down and should not be called again.
  term,
};

} // namespace broker::internal
