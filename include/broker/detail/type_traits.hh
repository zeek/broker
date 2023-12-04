#pragma once

#include <cstddef>
#include <list>
#include <map>
#include <optional>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <vector>

namespace broker {

class data;
class status;
class topic;

} // namespace broker

namespace broker::detail {

template <class>
struct is_variant_oracle : std::false_type {};

template <class... Ts>
struct is_variant_oracle<std::variant<Ts...>> : std::true_type {};

template <class T>
inline constexpr bool is_variant = is_variant_oracle<T>::value;

template <class>
struct is_optional_oracle : std::false_type {};

template <class T>
struct is_optional_oracle<std::optional<T>> : std::true_type {};

template <class T>
inline constexpr bool is_optional = is_optional_oracle<T>::value;

template <class>
struct is_array_oracle : std::false_type {};

template <class T, size_t N>
struct is_array_oracle<std::array<T, N>> : std::true_type {};

template <class T, size_t N>
struct is_array_oracle<T[N]> : std::true_type {};

template <class T>
inline constexpr bool is_array = is_array_oracle<T>::value;

template <class>
struct is_map_oracle : std::false_type {};

template <class Key, class Val>
struct is_map_oracle<std::map<Key, Val>> : std::true_type {};

template <class Key, class Val>
struct is_map_oracle<std::unordered_map<Key, Val>> : std::true_type {};

template <class T>
inline constexpr bool is_map = is_map_oracle<T>::value;

template <class>
struct is_list_oracle : std::false_type {};

template <class T>
struct is_list_oracle<std::list<T>> : std::true_type {};

template <class T>
struct is_list_oracle<std::vector<T>> : std::true_type {};

template <class T>
inline constexpr bool is_list = is_list_oracle<T>::value;

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
  static constexpr bool value = //
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

#define BROKER_DEF_HAS_ENCODE_IN_NS(ns_name)                                   \
  template <class T, class OutIter>                                            \
  class has_encode_overload {                                                  \
  private:                                                                     \
    template <class U>                                                         \
    static auto sfinae(U& y)                                                   \
      -> decltype(::ns_name::encode(y, std::declval<OutIter&>()),              \
                  std::true_type{});                                           \
    static std::false_type sfinae(...);                                        \
    using result_type = decltype(sfinae(std::declval<T&>()));                  \
                                                                               \
  public:                                                                      \
    static constexpr bool value = result_type::value;                          \
  };                                                                           \
  template <class T, class OutIter>                                            \
  inline constexpr bool has_encode_overload_v =                                \
    has_encode_overload<T, OutIter>::value
