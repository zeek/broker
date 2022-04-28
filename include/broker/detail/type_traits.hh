#pragma once

#include <cstddef>

#include <type_traits>

namespace broker {

class data;
class status;
class topic;

namespace detail {

template <class>
struct always_false : std::false_type {};

template <class T>
inline constexpr bool always_false_v = always_false<T>::value;

// std::enable_if_t shortcut from C++14.
template <bool B, class T = void>
using enable_if_t = typename std::enable_if<B, T>::type;

// std::conditional_t shortcut from C++14.
template <bool B, class T, class F>
using conditional_t = typename std::conditional<B, T, F>::type;

// std::conjunction from C++17.
template <class...>
struct conjunction : std::true_type {};

template <class B1>
struct conjunction<B1> : B1 { };

template <class B1, class... Bn>
struct conjunction<B1, Bn...>
  : conditional_t<B1::value != false, conjunction<Bn...>, B1>  {};

// std::remove_reference_t shortcut from C++14.
template <class T>
using remove_reference_t = typename std::remove_reference<T>::type;

// std::decay_t shortcut from C++14.
template <class T>
using decay_t = typename std::decay<T>::type;

// std::aligned_storage_t shortcut from C++14.
template <size_t Len, size_t Align>
using aligned_storage_t = typename std::aligned_storage<Len, Align>::type;

template <bool B, class T = void>
using disable_if = std::enable_if<!B, T>;

template <bool B, class T = void>
using disable_if_t = typename disable_if<B, T>::type;

template <class A, class B>
using is_same_or_derived = std::is_base_of<A, remove_reference_t<B>>;

template <class A, class B>
using disable_if_same_or_derived = disable_if<is_same_or_derived<A, B>::value>;

template <class A, class B>
using disable_if_same_or_derived_t =
  typename disable_if_same_or_derived<A, B>::type;

template <template <class> class F, class Head>
constexpr decltype(F<Head>::value) max() {
  return F<Head>::value;
}

template <template <class> class F, class Head, class Next, class... Tail>
constexpr decltype(F<Head>::value) max() {
  return max<F, Head>() > max<F, Next, Tail...>() ? max<F, Head>() :
                                                    max<F, Next, Tail...>();
}

// A variadic extension of std::is_same.
template <class... Ts> struct are_same;

template <>
struct are_same<> : std::true_type {};

template <class T>
struct are_same<T> : std::true_type {};

template <class T0, class T1, class... Ts>
struct are_same<T0, T1, Ts...> :
  conditional_t<
    std::is_same<T0, T1>::value,
    are_same<T1, Ts...>,
    std::false_type
  > {};

template <class... Ts>
inline constexpr bool are_same_v = are_same<Ts...>::value;

// Trait that checks for an overload of convert(const From&, T&).
template <class From, class To>
struct has_convert {
  using from_type = decay_t<From>;
  using to_type = typename std::add_lvalue_reference<decay_t<To>>::type;

  template <class T>
  static auto test(T* x)
  -> decltype(convert(*x, std::declval<to_type>()), std::true_type());

  template <class T>
  static auto test(...) -> std::false_type;

  using type = decltype(test<from_type>(nullptr));
  static constexpr bool value = type::value;
};

template <class T, size_t = sizeof(T)>
std::true_type is_complete_test(T*);

std::false_type is_complete_test(...);

/// Checks whether `T` is complete type. Passing a forward declaration or
/// undefined template specialization evaluates to `false`.
template <class T>
inline constexpr bool is_complete
  = decltype(is_complete_test(std::declval<T*>()))::value;

template <class... Ts>
struct type_list {};

template <class U>
auto has_apply_operator_test(U*) -> decltype(&U::operator(), std::true_type());

auto has_apply_operator_test(...) -> std::false_type;

template <class T>
inline constexpr bool has_apply_operator
  = decltype(has_apply_operator_test(std::declval<T*>()))::value;

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

} // namespace detail
} // namespace broker
