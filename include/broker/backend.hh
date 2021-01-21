#pragma once

#include <cstdint>

namespace broker {

/// Describes the supported data store backend.
enum class backend : uint8_t {
  memory, ///< An in-memory backend based on a simple hash table.
  sqlite, ///< A SQLite3 backend.
};

/// @relates backend
template <class Inspector>
bool inspect(Inspector& f, backend& x) {
  auto get = [&] { return static_cast<uint8_t>(x); };
  auto set = [&](uint8_t val) {
    if (val <= static_cast<uint8_t>(backend::sqlite)) {
      x = static_cast<backend>(val);
      return true;
    } else {
      return false;
    }
  };
  return f.apply(get, set);
}

} // namespace broker
