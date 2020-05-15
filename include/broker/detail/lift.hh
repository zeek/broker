#pragma once

#include <type_traits>

#include "broker/none.hh"

namespace broker::detail {

template <class... AtomPrefix>
struct lift_helper {
  template <class T, class U, class R, class... Ts>
  auto operator()(T& obj, R (U::*fun)(Ts...)) const {
    return [&obj, fun](AtomPrefix..., Ts... xs) { return (obj.*fun)(xs...); };
  }
};

/// Lifts a member function pointer to a message handler, prefixed with
/// `AtomPrefix`.
template <class... AtomPrefix>
constexpr lift_helper<AtomPrefix...> lift = lift_helper<AtomPrefix...>{};

} // namespace broker::detail
