#pragma once

#include <cstdint>
#include <string>
#include <tuple>

#include <caf/meta/type_name.hpp>
#include <caf/node_id.hpp>

#include "broker/detail/hash.hh"

namespace broker {

/// Uniquely identifies a *publisher* in the distributed system.
struct publisher_id {
  /// Identifies the @ref endpoint instance that hosts the *publisher*.
  caf::node_id endpoint;

  /// Identifies the local object that published a message, data store change,
  /// or event. Usually, this ID belongs to a @ref publisher or @ref store
  /// object. The @ref endpoint sets this ID to 0 when referring to itself,
  /// e.g., when using `endpoint::publish`.
  uint64_t object;

  /// Returns whether this ID is valid, i.e., whether the `endpoint` member is
  /// valid.
  explicit operator bool() const noexcept {
    return static_cast<bool>(endpoint);
  }
};

/// @relates publisher_id
template <class Inspector>
bool inspect(Inspector& f, publisher_id& x) {
  return f.object(x).fields(f.field("endpoint", x.endpoint),
                            f.field("object", x.object));
}

/// @relates publisher_id
inline bool operator==(const publisher_id& x, const publisher_id& y) noexcept {
  return std::tie(x.endpoint, x.object) == std::tie(y.endpoint, y.object);
}

/// @relates publisher_id
inline bool operator!=(const publisher_id& x, const publisher_id& y) noexcept {
  return !(x == y);
}

/// @relates publisher_id
inline bool operator<(const publisher_id& x, const publisher_id& y) noexcept {
  return std::tie(x.endpoint, x.object) < std::tie(y.endpoint, y.object);
}

/// @relates publisher_id
std::string to_string(const publisher_id& x);

} // namespace broker

namespace std {

template <>
struct hash<broker::publisher_id> {
  size_t operator()(const broker::publisher_id& x) const noexcept {
    // TODO: use caf::hash::fnv when switching to CAF 0.18.
    hash<caf::node_id> f;
    auto result = f(x.endpoint);
    broker::detail::hash_combine(result, x.object);
    return result;
  }
};

} // namespace std
