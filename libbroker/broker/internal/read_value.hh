#pragma once

#include <caf/error.hpp>

namespace broker::internal {

// Utility class for reading a primitive value from an inspector (source). If T
// is an enum, then this function performs (unsafe) static casts. This utility
// should eventually get the axe, it only makes transitioning from 0.17 to 0.18
// easier since Broker still uses CAF 0.17-style `if (auto err = ...)` a lot.
template <class Source, class T>
caf::error read_value(Source& source, T& storage) {
  if constexpr (std::is_enum_v<T>) {
    auto tmp = std::underlying_type_t<T>{};
    if (source.value(tmp)) {
      storage = static_cast<T>(tmp);
      return {};
    } else {
      return source.get_error();
    }
  } else {
    if (source.value(storage))
      return {};
    else
      return source.get_error();
  }
}

} // namespace broker::internal
