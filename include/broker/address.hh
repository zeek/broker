#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>

#include "broker/detail/comparable.hh"

namespace broker {

/// Stores an IPv4 or IPv6 address.
class address : detail::comparable<address> {
public:
  static constexpr size_t num_bytes = 16;

  using array_type = std::array<uint8_t, num_bytes>;

  /// Distinguishes between address types.
  enum class family : uint8_t {
    ipv4,
    ipv6,
  };

  /// Distinguishes between address byte ordering.
  enum class byte_order : uint8_t {
    host,
    network,
  };

  address() noexcept;

  address(const address&) noexcept;

  /// Construct an address from raw bytes.
  /// @param bytes A pointer to the raw representation.  This must point
  /// to 4 bytes if *fam* is `family::ipv4` and 16 bytes for `family::ipv6`.
  /// @param fam The type of address.
  /// @param order The byte order in which *bytes* is stored.
  address(const uint32_t* bytes, family fam, byte_order order);

  address& operator=(const address&) noexcept;

  /// Mask out lower bits of the address.
  /// @param top_bits_to_keep The number of bits to *not* mask out, counting
  /// from the highest order bit.  The value is always interpreted relative to
  /// the IPv6 bit width, even if the address is IPv4.  That means to compute
  /// 192.168.1.2/16, pass in 112 (i.e. 96 + 16).  The value must range from
  /// 0 to 128.
  /// @returns true on success.
  bool mask(uint8_t top_bits_to_keep);

  /// @returns true if the address is IPv4.
  bool is_v4() const noexcept;

  /// @returns true if the address is IPv6.
  bool is_v6() const noexcept {
    return !is_v4();
  }

  /// @returns the raw bytes of the address in network order. For IPv4
  /// addresses, this uses the IPv4-mapped IPv6 address representation.
  array_type& bytes() noexcept {
    return bytes_;
  }

  /// @returns the raw bytes of the address in network order. For IPv4
  /// addresses, this uses the IPv4-mapped IPv6 address representation.
  const std::array<uint8_t, 16>& bytes() const noexcept {
    return bytes_;
  }

  [[nodiscard]] int compare(const address& other) const noexcept;

  [[nodiscard]] size_t hash() const;

  // -- conversion support -----------------------------------------------------

  void convert_to(std::string& str) const;

  [[nodiscard]] bool convert_from(const std::string& str);

private:
  array_type bytes_;
};

// -- free functions -----------------------------------------------------------

/// @relates address
inline bool convert(const std::string& str, address& a) {
  return a.convert_from(str);
}

/// @relates address
inline void convert(const address& a, std::string& str) {
  a.convert_to(str);
}

/// @relates address
template <class Inspector>
bool inspect(Inspector& f, address& x) {
  if (f.has_human_readable_format()) {
    auto get = [&] {
      std::string str;
      convert(x, str);
      return str;
    };
    auto set = [&](const std::string& str) { return convert(str, x); };
    return f.apply(get, set);
  } else {
    return f.object(x).fields(f.field("bytes", x.bytes()));
  }
}

} // namespace broker

namespace std {

/// @relates address
template <>
struct hash<broker::address> {
  size_t operator()(const broker::address& x) const {
    return x.hash();
  }
};

} // namespace std
