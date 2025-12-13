#pragma once

#include "broker/fwd.hh"

#include <cstddef>
#include <iterator>
#include <list>
#include <map>
#include <optional>
#include <set>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

namespace caf {
enum class byte : uint8_t;
}

namespace broker {

class data;
class status;
class topic;

} // namespace broker

namespace broker::detail {

template <class>
struct is_duration_oracle : std::false_type {};

template <class Rep, class Period>
struct is_duration_oracle<std::chrono::duration<Rep, Period>> : std::true_type {
};

template <class T>
inline constexpr bool is_duration = is_duration_oracle<T>::value;

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
struct is_set_oracle : std::false_type {};

template <class Key, class Val>
struct is_set_oracle<std::set<Key, Val>> : std::true_type {};

template <class Key, class Val>
struct is_set_oracle<std::unordered_set<Key, Val>> : std::true_type {};

template <class T>
inline constexpr bool is_set = is_set_oracle<T>::value;

template <class>
struct is_list_oracle : std::false_type {};

template <class T>
struct is_list_oracle<std::list<T>> : std::true_type {};

template <class T>
struct is_list_oracle<std::vector<T>> : std::true_type {};

template <class T>
inline constexpr bool is_list = is_list_oracle<T>::value;

template <class...>
struct parameter_pack {};

template <class T>
struct tag {
  using type = T;
};

template <class>
struct always_false : std::false_type {};

template <class T>
inline constexpr bool always_false_v = always_false<T>::value;

/// Checks whether T is one of the types in the template parameter pack Ts.
template <class T, class... Ts>
inline constexpr bool is_one_of_v = (std::is_same_v<T, Ts> || ...);

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

template <class T>
concept bool_or_void = is_one_of_v<T, bool, void>;

template <class From, class To>
concept convertible = requires(const From& from, To& to) {
  { convert(from, to) } -> bool_or_void;
};

template <class T>
concept member_function_pointer = std::is_member_function_pointer_v<T>;

template <class T>
concept applicable = requires(T) {
  { &T::operator() } -> member_function_pointer;
};

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

template <class Fn>
struct signature_of_oracle {
  using type = normalized_signature_t<Fn>;
};

template <applicable Fn>
struct signature_of_oracle<Fn> {
  using type = normalized_signature_t<decltype(&Fn::operator())>;
};

template <class F>
using signature_of_t = typename signature_of_oracle<F>::type;

template <class T>
concept iterable = requires(T t) {
  { t.begin() } -> std::input_iterator;
  { t.end() } -> std::input_iterator;
};

// Trait that checks whether T is a std::pair.
template <class T>
struct is_pair_oracle : std::false_type {};

template <class T, class U>
struct is_pair_oracle<std::pair<T, U>> : std::true_type {};

template <class T>
inline constexpr bool is_pair = is_pair_oracle<T>::value;

// Trait that checks whether T is a std::tuple.
template <class T>
struct is_tuple_oracle : std::false_type {};

template <class... Ts>
struct is_tuple_oracle<std::tuple<Ts...>> : std::true_type {};

template <class T>
inline constexpr bool is_tuple = is_tuple_oracle<T>::value;

template <class T>
concept has_to_string = requires(const T& t) {
  { to_string(t) } -> std::convertible_to<std::string>;
};

template <class T>
concept has_string_getter = requires(const T& t) {
  { t.string() } -> std::convertible_to<std::string_view>;
};

template <class T>
concept byte_output_iterator =
  std::output_iterator<T, std::byte> || std::output_iterator<T, caf::byte>;

template <class T>
concept char_output_iterator = std::output_iterator<T, char>;

template <class T>
concept arithmetic = std::is_arithmetic_v<T>;

} // namespace broker::detail

#define BROKER_DEF_HAS_ENCODE_IN_NS(ns_name)                                   \
  template <class T, class OutIter>                                            \
  concept has_encode_overload = requires(T t, OutIter out) {                   \
    { ::ns_name::encode(t, out) } -> std::same_as<OutIter>;                    \
  };
