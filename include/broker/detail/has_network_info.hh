#pragma once

#include <type_traits>

#include "broker/error.hh"
#include "broker/status.hh"

namespace broker::detail {

/// Convenience metaprogramming utility that evaluates to either
/// `sc_has_network_info_v` or `ec_has_network_info_v`.
template <class EnumConstant>
struct has_network_info;

template <sc S>
struct has_network_info<std::integral_constant<sc, S>> {
  static constexpr bool value = sc_has_network_info_v<S>;
};

template <ec E>
struct has_network_info<std::integral_constant<ec, E>> {
  static constexpr bool value = ec_has_network_info_v<E>;
};

/// @relates has_network_info
template <class EnumConstant>
constexpr bool has_network_info_v = has_network_info<EnumConstant>::value;

} // namespace broker::detail
