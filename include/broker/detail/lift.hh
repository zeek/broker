#pragma once

#include <type_traits>

#include "broker/error.hh"
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

template <class... AtomPrefix>
struct drop_helper {
  template <class U, class R, class... Ts>
  auto operator()(R (U::*fun)(Ts...)) const {
    return [](AtomPrefix..., Ts... xs) {};
  }
};

/// Deduces the signature from a message handler (prefixed with `AtomPrefix`)
/// but returns a lambda with an empty body.
template <class... AtomPrefix>
constexpr drop_helper<AtomPrefix...> drop = drop_helper<AtomPrefix...>{};

template <ec Code,class... AtomPrefix>
struct reject_helper {
  template <class U, class R, class... Ts>
  auto operator()(R (U::*fun)(Ts...)) const {
    return [](AtomPrefix..., Ts... xs) -> error { return Code; };
  }
};

/// Deduces the signature from a message handler (prefixed with `AtomPrefix`)
/// but returns a lambda with an empty body.
template <ec Code, class... AtomPrefix>
constexpr reject_helper<Code, AtomPrefix...> reject
  = reject_helper<Code, AtomPrefix...>{};

} // namespace broker::detail
