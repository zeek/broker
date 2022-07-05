#pragma once

#include "broker/error.hh"

namespace broker::internal {

// Utility class for writing a primitive value from an inspector (source). If T
// is an enum, then this function performs (unsafe) static casts. This utility
// should eventually get the axe, it only makes transitioning from 0.17 to 0.18
// easier since Broker still uses CAF 0.17-style `if (auto err = ...)` a lot.
template <class Sink, class T>
caf::error write_value(Sink& sink, const T& x) {
  if constexpr (std::is_enum_v<T>) {
    auto tmp = static_cast<std::underlying_type_t<T>>(x);
    if (sink.value(tmp))
      return {};
    else
      return sink.get_error();

  } else {
    if (sink.value(x))
      return {};
    else
      return sink.get_error();
  }
}

} // namespace broker::internal
