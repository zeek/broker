#pragma once

#include "broker/endpoint_id.hh"
#include "broker/fwd.hh"

#include <cstdint>
#include <functional>
#include <string>

namespace broker {

/// Uniquely identifies a *publisher* in the distributed system.
struct entity_id {
  /// Identifies the @ref endpoint instance that hosts the *publisher*.
  endpoint_id endpoint;

  /// Identifies the local object that published a message, data store change,
  /// or event. Usually, this ID belongs to a @ref publisher or @ref store
  /// object. The @ref endpoint sets this ID to 0 when referring to itself,
  /// e.g., when using `endpoint::publish`.
  uint64_t object = 0;

  /// Returns whether this ID is valid, i.e., whether the `endpoint` member is
  /// valid.
  bool valid() const noexcept {
    return static_cast<bool>(endpoint);
  }

  /// @copydoc valid
  explicit operator bool() const noexcept {
    return valid();
  }

  /// Returns `!valid()`.
  bool operator!() const noexcept {
    return !valid();
  }

  /// Returns an invalid ID.
  static entity_id nil() noexcept {
    return {endpoint_id{}, 0};
  }

  /// Converts the handle type to an entity ID.
  template <class Handle>
  static entity_id from(endpoint_id ep, const Handle& hdl) {
    return hdl ? entity_id{ep, hdl->id()} : nil();
  }

  /// Computes a hash value for this object.
  size_t hash() const noexcept;
};

/// @relates entity_id
template <class Inspector>
bool inspect(Inspector& f, entity_id& x) {
  return f.object(x)
    .pretty_name("entity_id")
    .fields(f.field("endpoint", x.endpoint), f.field("object", x.object));
}

/// @relates entity_id
inline bool operator==(const entity_id& x, const entity_id& y) noexcept {
  return std::tie(x.endpoint, x.object) == std::tie(y.endpoint, y.object);
}

/// @relates entity_id
inline bool operator!=(const entity_id& x, const entity_id& y) noexcept {
  return !(x == y);
}

/// @relates entity_id
inline bool operator<(const entity_id& x, const entity_id& y) noexcept {
  return std::tie(x.endpoint, x.object) < std::tie(y.endpoint, y.object);
}

/// @relates entity_id
void convert(const entity_id& in, std::string& out);

} // namespace broker

namespace std {

template <>
struct hash<broker::entity_id> {
  size_t operator()(const broker::entity_id& x) const noexcept {
    return x.hash();
  }
};

} // namespace std
