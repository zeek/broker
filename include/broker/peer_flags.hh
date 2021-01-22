#pragma once

namespace broker {

/// Describes the type of peering.
enum class peer_flags : int {
  invalid = 0x00,
  local = 0x01,
  remote = 0x02,
  outbound = 0x04,
  inbound = 0x08,
};

/// @relates peer_flags
constexpr peer_flags operator+(peer_flags lhs, peer_flags rhs) {
  return static_cast<peer_flags>(static_cast<int>(lhs) | static_cast<int>(rhs));
}

/// @relates peer_flags
constexpr bool is_invalid(peer_flags p) {
  return (static_cast<int>(p) & static_cast<int>(peer_flags::invalid)) != 0;
}

/// @relates peer_flags
constexpr bool is_local(peer_flags p) {
  return (static_cast<int>(p) & static_cast<int>(peer_flags::local)) != 0;
}

/// @relates peer_flags
constexpr bool is_remote(peer_flags p) {
  return (static_cast<int>(p) & static_cast<int>(peer_flags::remote)) != 0;
}

/// @relates peer_flags
constexpr bool is_outbound(peer_flags p) {
  return (static_cast<int>(p) & static_cast<int>(peer_flags::outbound)) != 0;
}

/// @relates peer_flags
constexpr bool is_inbound(peer_flags p) {
  return (static_cast<int>(p) & static_cast<int>(peer_flags::inbound)) != 0;
}

/// @relates peer_flags
template <class Inspector>
bool inspect(Inspector& f, peer_flags& x) {
  auto get = [&] { return static_cast<int>(x); };
  auto set = [&](int val) {
    if ((val & 0x0F) == val) {
      x = static_cast<peer_flags>(val);
      return true;
    } else {
      return false;
    }
  };
  return f.apply(get, set);
}

} // namespace broker
