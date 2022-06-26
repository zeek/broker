#pragma once

#include <functional>
#include <string>

namespace broker {

/// An empty class with a single instance only.
struct none {};

/// @relates none
inline constexpr bool operator==(none, none) noexcept {
  return true;
}

/// @relates none
inline constexpr bool operator!=(none, none) noexcept {
  return false;
}

/// @relates none
inline constexpr bool operator<(none, none) noexcept {
  return false;
}

/// @relates none
inline constexpr bool operator>(none, none) noexcept {
  return false;
}

/// @relates none
inline constexpr bool operator<=(none, none) noexcept {
  return true;
}

/// @relates none
inline constexpr bool operator>=(none, none) noexcept {
  return true;
}

/// @relates none
inline void convert(none, std::string& str) {
  str = "nil";
}

/// The only instance of @ref none.
/// @relates none
constexpr auto nil = none{};

} // namespace broker

namespace std {

template <>
struct hash<broker::none> {
  using result_type = size_t;

  result_type operator()(broker::none) const {
    return 0;
  }
};

} // namespace std
