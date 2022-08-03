#pragma once

#include <cstddef>

#include <type_traits>

namespace broker {

class data;
class status;
class topic;

} // namespace broker

namespace broker::detail {

template <class T>
struct tag {
  using type = T;
};

template <class>
struct always_false : std::false_type {};

template <class T>
inline constexpr bool always_false_v = always_false<T>::value;

// A variadic extension of std::is_same.
template <class... Ts>
struct are_same;

template <class A, class B>
struct are_same<A, B> {
  static constexpr bool value = std::is_same_v<A, B>;
};

template <class A, class B, class C, class... Ts>
struct are_same<A, B, C, Ts...> {
  static constexpr bool value =
    std::is_same_v<A, B> && are_same<B, C, Ts...>::value;
};

template <class... Ts>
inline constexpr bool are_same_v = are_same<Ts...>::value;

// Trait that checks for an overload of convert(const From&, T&).
template <class From, class To>
struct has_convert {
  template <class T>
  static auto test(const T* x)
    -> decltype(convert(*x, std::declval<To&>()), std::true_type());

  template <class T>
  static auto test(...) -> std::false_type;

  static constexpr bool value = decltype(test<From>(nullptr))::value;
};

template <class From, class To>
inline constexpr bool has_convert_v = has_convert<From, To>::value;

template <class U>
auto has_apply_operator_test(U*) -> decltype(&U::operator(), std::true_type());

auto has_apply_operator_test(...) -> std::false_type;

template <class T>
inline constexpr bool has_apply_operator =
  decltype(has_apply_operator_test(std::declval<T*>()))::value;

template <class F>
struct normalized_signature;

template <class R, class... Ts>
struct normalized_signature<R(Ts...)> {
  using type = R(Ts...);
};

template <class R, class... Ts>
struct normalized_signature<R(Ts...) noexcept> {
  using type = R(Ts...);
};

template <class R, class... Ts>
struct normalized_signature<R (*)(Ts...)> {
  using type = R(Ts...);
};

template <class R, class... Ts>
struct normalized_signature<R (*)(Ts...) noexcept> {
  using type = R(Ts...);
};

template <class C, typename R, class... Ts>
struct normalized_signature<R (C::*)(Ts...)> {
  using type = R(Ts...);
};

template <class C, typename R, class... Ts>
struct normalized_signature<R (C::*)(Ts...) const> {
  using type = R(Ts...);
};

template <class C, typename R, class... Ts>
struct normalized_signature<R (C::*)(Ts...) noexcept> {
  using type = R(Ts...);
};

template <class C, typename R, class... Ts>
struct normalized_signature<R (C::*)(Ts...) const noexcept> {
  using type = R(Ts...);
};

template <class F>
using normalized_signature_t = typename normalized_signature<F>::type;

template <class F, bool HasApplyOperator = has_apply_operator<F>>
struct signature_of_oracle;

template <class F>
struct signature_of_oracle<F, false> {
  using type = normalized_signature_t<F>;
};

template <class F>
struct signature_of_oracle<F, true> {
  using type = normalized_signature_t<decltype(&F::operator())>;
};

template <class F>
using signature_of_t = typename signature_of_oracle<F>::type;

} // namespace broker::detail
