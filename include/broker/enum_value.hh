#pragma once

#include <functional>
#include <ostream>
#include <string>

#include "broker/detail/operators.hh"

namespace broker {

/// Stores the name of an enum value.  The receiver is responsible for knowing
/// how to map the name to the actual value if it needs that information.
struct enum_value : detail::totally_ordered<enum_value> {
  /// Default construct empty enum value name.
  enum_value() = default;

  /// Construct enum value from a string.
  explicit enum_value(std::string name) : name{std::move(name)} {
    // nop
  }

  std::string name;
};

/// @relates enum_value
inline bool operator==(const enum_value& lhs, const enum_value& rhs) {
  return lhs.name == rhs.name;
}

/// @relates enum_value
inline bool operator<(const enum_value& lhs, const enum_value& rhs) {
  return lhs.name < rhs.name;
}

/// @relates enum_value
template <class Inspector>
bool inspect(Inspector& f, enum_value& e) {
  return f.apply(e.name);
}

/// @relates enum_value
inline void convert(const enum_value& e, std::string& str) {
  str = e.name;
}

} // namespace broker

namespace std {

template <>
struct hash<broker::enum_value> {
  size_t operator()(const broker::enum_value& v) const {
    return std::hash<std::string>{}(v.name);
  }
};

} // namespace std
