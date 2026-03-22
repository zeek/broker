#pragma once

namespace broker::internal {

/// Indicates the state of unpeering a peer.
enum class connection_status {
  /// We are not unpeering from this peer.
  alive,
  /// We are unpeering from this peer gracefully.
  unpeering,
  /// The connection has been forcibly closed.
  connection_lost,
  /// We are unpeering from this peer due to an overflow disconnect.
  overflow_disconnect,
};

} // namespace broker::internal
