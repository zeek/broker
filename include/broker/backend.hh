#pragma once

#include <cstdint>
#include <string>
#include <type_traits>

#include "broker/detail/enum_inspect.hh"

namespace broker {

/// Describes the supported data store backend.
enum class backend : uint8_t {
  memory, ///< An in-memory backend based on a simple hash table.
  sqlite, ///< A SQLite3 backend.
};

/// Convenience alias for the underlying integer type of @ref backend.
using backend_ut = std::underlying_type_t<backend>;

// -- conversion and inspection support ----------------------------------------

/// @relates backend
bool convert(backend src, backend_ut& dst) noexcept;

/// @relates backend
bool convert(backend src, std::string& dst);

/// @relates backend
bool convert(backend_ut src, backend& dst) noexcept;

/// @relates backend
bool convert(const std::string& src, backend& dst) noexcept;

/// @relates backend
std::string to_string(backend code);

/// @relates backend
template <class Inspector>
bool inspect(Inspector& f, backend& x) {
  return detail::enum_inspect(f, x);
}

} // namespace broker
