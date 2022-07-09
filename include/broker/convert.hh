#pragma once

#include <chrono>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>

#include "broker/detail/type_traits.hh"
#include "broker/fwd.hh"

namespace broker::detail {

bool can_convert_data_to_node(const data& src);

} // namespace broker::detail

namespace broker {

/// Customization point for extending `can_convert`.
template <class T>
struct can_convert_predicate;

// Enable `can_convert` for `endpoint_id`.
template <>
struct can_convert_predicate<endpoint_id> {
  static bool check(const data& src) {
    return detail::can_convert_data_to_node(src);
  }
};

template <class T, class U>
auto can_convert_to(const U& x)
  -> decltype(can_convert_predicate<T>::check(x)) {
  return can_convert_predicate<T>::check(x);
}

template <class Rep>
void convert_duration(Rep count, const char* unit_name, std::string& str) {
  str = std::to_string(count);
  str += unit_name;
}

template <class Rep>
void convert(std::chrono::duration<Rep, std::nano> d, std::string& str) {
  convert_duration(d.count(), "ns", str);
}

template <class Rep>
void convert(std::chrono::duration<Rep, std::micro> d, std::string& str) {
  convert_duration(d.count(), "us", str);
}

template <class Rep>
void convert(std::chrono::duration<Rep, std::milli> d, std::string& str) {
  convert_duration(d.count(), "ms", str);
}

template <class Rep>
void convert(std::chrono::duration<Rep, std::ratio<1>> d, std::string& str) {
  convert_duration(d.count(), "s", str);
}

template <class Rep>
void convert(std::chrono::duration<Rep, std::ratio<60>> d, std::string& str) {
  convert_duration(d.count(), "mins", str);
}

template <class Rep>
void convert(std::chrono::duration<Rep, std::ratio<3600>> d, std::string& str) {
  convert_duration(d.count(), "hrs", str);
}

/// Attempts to convert `From` to `To`.
/// @returns a value of type `To` on success, otherwise `nil`.
template <class To, class From>
std::enable_if_t<detail::has_convert_v<From, To>, std::optional<To>>
to(const From& from) {
  auto result = To{};
  using convert_res_type = decltype(convert(from, result));
  if constexpr (std::is_same_v<convert_res_type, bool>) {
    if (convert(from, result)) {
      return {std::move(result)};
    } else {
      return {};
    }
  } else {
    static_assert(std::is_same_v<convert_res_type, void>,
                  "convert overloads must return 'bool' or 'void'");
    convert(from, result);
    return {std::move(result)};
  }
}

/// Forces a conversion from `From` to `To`.
/// @returns a value of type `To`.
/// @throws logic_error if the conversion fails.
template <class To, class From>
std::enable_if_t<detail::has_convert_v<From, To>, To> get_as(const From& from) {
  To result;
  if (!convert(from, result))
    throw std::logic_error("conversion failed");
  return result;
}

/// Converts a value to a string representation.
template <class From>
std::enable_if_t<detail::has_convert_v<From, std::string>, std::string>
to_string(const From& from) {
  std::string result;
  convert(from, result);
  return result;
}

/// Convenience alias for `from<T>(str)`.
template <class T>
auto from_string(std::string_view str) -> decltype(to<T>(str)) {
  return to<T>(str);
}

// Injects an overload for `operator<<` for any type convertible to a
// `std::string` via a free function `bool convert(const T&, std::string&)`
// that can be found via ADL.
template <class T>
std::enable_if_t<
  detail::has_convert_v<T, std::string> // must have a convert function
    && !std::is_arithmetic_v<T> // avoid ambiguitiy with default overloads
    && !std::is_convertible_v<T, std::string>, // avoid ambiguity with
                                               // conversion-to-string
  std::ostream&>
operator<<(std::ostream& os, const T& x) {
  std::string str;
  convert(x, str);
  return os << str;
}

template <class Enum, size_t N>
bool default_enum_convert(const std::string_view (&values)[N],
                          std::string_view in, Enum& out) {
  for (size_t index = 0; index < N; ++index) {
    if (values[index] == in) {
      out = static_cast<Enum>(index);
      return true;
    }
  }
  return false;
}

} // namespace broker
