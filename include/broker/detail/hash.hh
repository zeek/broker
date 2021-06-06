#pragma once

#include <functional>

#include <caf/hash/fnv.hpp>

namespace broker::detail {

/// Calculate hash for an object and combine with a provided hash.
template <class T>
void hash_combine(size_t& seed, const T& v) {
  seed ^= std::hash<T>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

struct fnv {
  template <class... Ts>
  size_t operator()(const Ts&... xs) const noexcept {
    return caf::hash::fnv<size_t>::compute(xs...);
  }
};

} // namespace broker::detail
