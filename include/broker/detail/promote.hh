#pragma once

#include "broker/fwd.hh"

#include <string>
#include <string_view>
#include <type_traits>

namespace broker::detail {

/// Promotes integral types to their corresponding Broker types, i.e., signed
/// integer types to @ref integer and unsigned integer types to @ref count.
/// Further, converts @ref enum_value to @ref enum_value_view and @ref
/// std::string to @ref std::string_view. When not performing a conversion,
/// returns the argument unchanged as if calling `std::forward`.
template <class T>
constexpr decltype(auto) promote(std::remove_reference_t<T>& val) noexcept {
  using val_t = std::decay_t<T>;
  if constexpr (std::is_integral_v<val_t> && !std::is_same_v<val_t, bool>) {
    if constexpr (std::is_signed_v<val_t>)
      return static_cast<integer>(val);
    else
      return static_cast<count>(val);
  } else if constexpr (std::is_same_v<val_t, std::string>) {
    return std::string_view{val};
  } else if constexpr (std::is_same_v<val_t, enum_value>) {
    return enum_value_view{val.name};
  } else {
    return static_cast<T&&>(val);
  }
}

/// @copydoc promote
template <class T>
constexpr decltype(auto) promote(std::remove_reference_t<T>&& val) noexcept {
  using val_t = std::decay_t<T>;
  if constexpr (std::is_integral_v<val_t> && !std::is_same_v<val_t, bool>) {
    if constexpr (std::is_signed_v<val_t>)
      return static_cast<integer>(val);
    else
      return static_cast<count>(val);
  } else if constexpr (std::is_same_v<val_t, std::string>) {
    return std::string_view{val};
  } else if constexpr (std::is_same_v<val_t, enum_value>) {
    return enum_value_view{val.name};
  } else {
    static_assert(!std::is_lvalue_reference_v<T>,
                  "cannot forward an rvalue as an lvalue");
    return static_cast<T&&>(val);
  }
}

} // namespace broker::detail
