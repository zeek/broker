#pragma once

#include <cstddef>
#include <limits>

#include "broker/config.hh"

namespace broker::detail {

#ifdef BROKER_WINDOWS

using native_socket = size_t;

constexpr native_socket invalid_native_socket =
  std::numeric_limits<native_socket>::max();

#else // BROKER_WINDOWS

using native_socket = int;

constexpr native_socket invalid_native_socket = -1;

#endif // BROKER_WINDOWS

} // namespace broker::detail
