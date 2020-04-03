#pragma once

#include <chrono>
#include <ostream>
#include <string>
#include <type_traits>

#include "broker/optional.hh"

#include "broker/detail/type_traits.hh"

namespace broker {

/// Customization point for extending `can_convert`.
template <class T>
struct can_convert_predicate;

template <class T, class U>
auto can_convert_to(const U& x)
-> decltype(can_convert_predicate<T>::check(x)) {
  return can_convert_predicate<T>::check(x);
}

template <class T>
auto convert(T x, std::string& str)
-> std::enable_if_t<std::is_arithmetic<T>::value, bool> {
  str = std::to_string(x);
  return true;
}

template <class Rep>
bool convert_duration(Rep count, const char* unit_name, std::string& str) {
  str = std::to_string(count);
  str += unit_name;
  return true;
}

template <class Rep>
bool convert(std::chrono::duration<Rep, std::nano> d, std::string& str) {
  return convert_duration(d.count(), "ns", str);
}

template <class Rep>
bool convert(std::chrono::duration<Rep, std::micro> d, std::string& str) {
  return convert_duration(d.count(), "us", str);
}

template <class Rep>
bool convert(std::chrono::duration<Rep, std::milli> d, std::string& str) {
  return convert_duration(d.count(), "ms", str);
}

template <class Rep>
bool convert(std::chrono::duration<Rep, std::ratio<1>> d, std::string& str) {
  return convert_duration(d.count(), "s", str);
}

template <class Rep>
bool convert(std::chrono::duration<Rep, std::ratio<60>> d, std::string& str) {
  return convert_duration(d.count(), "mins", str);
}

template <class Rep>
bool convert(std::chrono::duration<Rep, std::ratio<3600>> d, std::string& str) {
  return convert_duration(d.count(), "hrs", str);
}

/// Attempts to convert `From` to `To`.
/// @returns a value of type `To` on success, otherwise `nil`.
template <class To, class From>
auto to(const From& from)
-> std::enable_if_t<detail::has_convert<From, To>::value, optional<To>> {
  To to;
  if (convert(from, to))
    return {std::move(to)};
  return {};
}

/// Forces a conversion from `From` to `To`.
/// @returns a value of type `To`.
/// @throws logic_error if the conversion fails.
template <class To, class From>
auto get_as(const From& from)
-> std::enable_if_t<detail::has_convert<From, To>::value, To> {
  To result;
  if (!convert(from, result))
    throw std::logic_error("conversion failed");
  return result;
}

// Injects a `to_string` overload for any type convertible to a `std::string`
// via a free function `bool convert(const T&, std::string&)` that can be
// found via ADL.
/// @relates from_string
template <class T>
auto to_string(T&& x)
-> decltype(convert(x, std::declval<std::string&>()), std::string()) {
  std::string str;
  convert(x, str);
  return str;
}

// The dual to `to_string`: it attempts to parse a type `T` from a
// `std::string`, given that it provides a free function `bool convert(const
// T&, std::string&)` that can be found via ADL.
/// @relates to_string
template <class T>
auto from_string(const std::string& str) -> decltype(to<T>(str)) {
  return to<T>(str);
}

// Injects an overload for `operator<<` for any type convertible to a
// `std::string` via a free function `bool convert(const T&, std::string&)`
// that can be found via ADL.
template <class Char, class Traits, class T>
auto operator<<(std::basic_ostream<Char, Traits>& os, T&& x)
-> detail::enable_if_t<
  detail::has_convert<T, std::string>::value
    && !std::is_same<T, std::string>::value,
  std::basic_ostream<Char, Traits>&
> {
  std::string str;
  if (convert(x, str))
    os << str;
  else
    os.setstate(std::ios::failbit);
  return os;
}

} // namespace broker
