#pragma once

#include "broker/detail/comparable.hh"

#include <cstddef>
#include <functional>
#include <utility>

namespace broker {

class endpoint_id : detail::comparable<endpoint_id> {
public:
  // -- member types -----------------------------------------------------------

  struct impl;

  endpoint_id() noexcept;

  endpoint_id(endpoint_id&&) noexcept;

  endpoint_id(const endpoint_id&) noexcept;

  explicit endpoint_id(const impl*) noexcept;

  endpoint_id& operator=(endpoint_id&&) noexcept;

  endpoint_id& operator=(const endpoint_id&) noexcept;

  ~endpoint_id();

  // -- properties -------------------------------------------------------------

  /// Queries whether this node is *not* default-constructed.
  bool valid() const noexcept;

  /// Queries whether this node is *not* default-constructed.
  explicit operator bool() const noexcept {
    return valid();
  }

  /// Queries whether this node is default-constructed.
  bool operator!() const noexcept {
    return !valid();
  }

  /// Exchanges the value of this object with `other`.
  void swap(endpoint_id& other) noexcept;

  /// Compares this instance to `other`.
  /// @returns -1 if `*this < other`, 0 if `*this == other`, and 1 otherwise.
  int compare(const endpoint_id& other) const noexcept;

  /// Returns a has value for the ID.
  size_t hash() const noexcept;

  /// Returns a pointer to the native representation.
  [[nodiscard]] impl* native_ptr() noexcept;

  /// Returns a pointer to the native representation.
  [[nodiscard]] const impl* native_ptr() const noexcept;

private:
  std::byte obj_[sizeof(impl*)];
};

std::string to_string(const endpoint_id& x);

} // namespace broker

namespace std {

template <>
struct hash<broker::endpoint_id> {
  size_t operator()(const broker::endpoint_id& x) const noexcept {
    return x.hash();
  }
};

} // namespace std
