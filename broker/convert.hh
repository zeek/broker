#ifndef BROKER_CONVERT_HH
#define BROKER_CONVERT_HH

#include <chrono>
#include <ostream>
#include <string>

#include "broker/optional.hh"

#include "broker/detail/type_traits.hh"

namespace broker {

template <class Rep, class Period>
bool convert(std::chrono::duration<Rep, Period> d, std::string& str) {
  str = std::to_string(d.count());
  if (std::is_same<Period, std::nano>::value)
    str += "ns";
  else if (std::is_same<Period, std::micro>::value)
    str += "us";
  else if (std::is_same<Period, std::milli>::value)
    str += "ms";
  else if (std::is_same<Period, std::ratio<1>>::value)
    str += "s";
  else if (std::is_same<Period, std::ratio<60>>::value)
    str += "mins";
  else if (std::is_same<Period, std::ratio<3600>>::value)
    str += "hrs";
  else
    return false;
  return true;
}

// Injects a `to<T>` overload for any type convertible to type `T` via a free
// function `bool convert(const From&, T&)` that can be found via ADL.
template <class To, class From>
auto to(From&& from)
-> detail::enable_if_t<detail::can_convert<From, To>::value, optional<To>> {
  To to;
  if (convert(from, to))
    return {std::move(to)};
  return {};
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
  detail::can_convert<T, std::string>::value
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

#endif // BROKER_CONVERT_HH
