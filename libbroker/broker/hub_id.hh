#pragma once

#include <cstdint>

namespace broker {

/// Strong type alias for uniquely identifying hubs.
enum class hub_id : uint64_t {
  /// Represents an invalid hub ID.
  invalid = 0,
};

template <class Inspector>
bool inspect(Inspector& f, hub_id& x) {
  if constexpr (Inspector::is_loading) {
    uint64_t tmp = 0;
    if (!f.apply(tmp))
      return false;
    x = static_cast<hub_id>(tmp);
    return true;
  } else {
    auto tmp = static_cast<uint64_t>(x);
    return f.apply(tmp);
  }
}

} // namespace broker
