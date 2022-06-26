#pragma once

#include "broker/convert.hh"
#include "broker/detail/comparable.hh"

#include <array>
#include <cstddef>
#include <cstring>
#include <functional>
#include <string>
#include <utility>

namespace broker {

/// A 16-byte universally unique identifier according to
/// [RFC 4122](https://tools.ietf.org/html/rfc4122).
class endpoint_id : detail::comparable<endpoint_id> {
public:
  // -- constants --------------------------------------------------------------

  static constexpr size_t num_bytes = 16;

  // -- member types -----------------------------------------------------------

  using array_type = std::array<std::byte, num_bytes>;

  endpoint_id() noexcept;

  explicit endpoint_id(const array_type& bytes) noexcept : bytes_(bytes) {
    // nop
  }

  endpoint_id(const endpoint_id&) noexcept = default;

  endpoint_id& operator=(const endpoint_id&) noexcept = default;

  // -- properties -------------------------------------------------------------

  /// Returns the individual bytes for the ID.
  const array_type& bytes() const noexcept {
    return bytes_;
  }

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

  /// Compares this instance to `other`.
  /// @returns -1 if `*this < other`, 0 if `*this == other`, and 1 otherwise.
  int compare(const endpoint_id& other) const noexcept {
    return memcmp(bytes_.data(), other.bytes_.data(), num_bytes);
  }

  /// Returns a has value for the ID.
  size_t hash() const noexcept;

  /// Creates a random endpoint_id.
  static endpoint_id random() noexcept;

  /// Creates a random endpoint_id with a predefined seed.
  static endpoint_id random(unsigned seed) noexcept;

  /// Convenience function for creating an endpoint_id with all 128 bits set to
  /// zero.
  static endpoint_id nil() noexcept {
    return endpoint_id{};
  }

  /// Queries whether `str` is convertible to an `endpoint_id`.
  static bool can_parse(const std::string& str);

  // -- inspection -------------------------------------------------------------

  template <class Inspector>
  friend bool inspect(Inspector& f, endpoint_id& x) {
    return f.apply(x.bytes_);
  }

private:
  array_type bytes_;
};

// -- free functions -----------------------------------------------------------

/// @relates endpoint_id
void convert(endpoint_id x, std::string& str);

/// @relates endpoint_id
bool convert(const std::string& str, endpoint_id& x);

} // namespace broker

namespace std {

template <>
struct hash<broker::endpoint_id> {
  size_t operator()(const broker::endpoint_id& x) const noexcept {
    return x.hash();
  }
};

} // namespace std
