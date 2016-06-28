#ifndef BROKER_DETAIL_TYPE_TRAITS_HH
#define BROKER_DETAIL_TYPE_TRAITS_HH

#include <type_traits>

namespace broker {
namespace detail {

// std::enable_if_t shortcut from C++14.
template <bool B, class T = void>
using enable_if_t = typename std::enable_if<B, T>::type;

// std::conditional_t shortcut from C++14.
template <bool B, class T, class F>
using conditional_t = typename std::conditional<B, T, F>::type;

// std::remove_reference_t shortcut from C++14.
template <class T>
using remove_reference_t = typename std::remove_reference<T>::type;

// std::decay_t shortcut from C++14.
template <class T>
using decay_t = typename std::decay<T>::type;

// std::aligned_storage_t shortcut from C++14.
template <std::size_t Len, std::size_t Align>
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

// Trait that checks for an overload of convert(const From&, T&).
template <class From, class To>
struct can_convert {
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

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_TYPE_TRAITS_HH
