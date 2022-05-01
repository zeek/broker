#pragma once

namespace broker {

/// Enumeration of socket options that Broker supports when listening for
/// incoming peerings. Note: the exact semantics on these flags depends on the
/// OS.
enum class sockopt {
  none = 0b0000,
  reuse_addr = 0b0001,
  reuse_port = 0b0010,
};

/// @relates sockopt
constexpr int to_integer(sockopt x) noexcept {
  return static_cast<int>(x);
}

/// @relates sockopt
constexpr sockopt operator|(sockopt x, sockopt y) noexcept {
  return sockopt{to_integer(x) | to_integer(y)};
}

/// @relates sockopt
constexpr sockopt operator&(sockopt x, sockopt y) noexcept {
  return sockopt{to_integer(x) & to_integer(y)};
}

/// @relates sockopt
constexpr sockopt operator~(sockopt x) noexcept {
  return sockopt{~to_integer(x)};
}

template <class Inspector>
bool inspect(Inspector& f, sockopt& x) {
  auto get = [&x] { return to_integer(x); };
  auto set = [&x](int val) { x = static_cast<sockopt>(val); };
  return f.apply(get, set);
}

} // namespace broker
