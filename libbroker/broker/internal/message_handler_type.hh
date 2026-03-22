#pragma once

namespace broker::internal {

/// Denotes the subtype of a handler.
enum class message_handler_type {
  store,
  master, // Subtype of store.
  clone,  // Subtype of store.
  client,
  peering,
  hub,
  subscriber, // Subtype of hub.
  publisher,  // Subtype of hub.
};

} // namespace broker::internal
