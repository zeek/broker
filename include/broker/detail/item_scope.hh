#pragma once

#include <cstdint>
#include <string>

namespace broker::detail {

/// Denotes the visibility scope of an @ref item.
enum class item_scope : uint8_t {
  global, /// Marks an item as visible to local subscribers and peers.
  local,  /// Marks an item as visible to local subscribers only.
  remote, /// Marks an item as visible to peers only.
};

/// @relates item_scope
std::string to_string(item_scope);

} // namespace broker::detail
