#pragma once

// Note: Broker no longer uses this header, but Zeek still depends on
// broker::detail::hash_combine.

#include <functional>

namespace broker::detail {

/// Calculate hash for an object and combine with a provided hash.
template <class T>
void hash_combine(size_t& seed, const T& v) {
  seed ^= std::hash<T>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

} // namespace broker::detail
