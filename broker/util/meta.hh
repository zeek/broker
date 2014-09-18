#ifndef BROKER_UTIL_META_HH
#define BROKER_UTIL_META_HH

#include <type_traits>

namespace broker {
namespace util {

// std::enable_if_t shortcut from C++14.
template <bool B, class T = void>
using enable_if_t = typename std::enable_if<B, T>::type;

// std::remove_reference_t shortcut from C++14.
template <class T>
using remove_reference_t = typename std::remove_reference<T>::type;

// std::aligned_union_t shortcut from C++14.
template <std::size_t Len, class... Types>
using aligned_union_t = typename std::aligned_union<Len, Types...>::type;

template <bool B, typename T = void>
using disable_if = std::enable_if<! B, T>;

template <bool B, typename T = void>
using disable_if_t = typename disable_if<B, T>::type;

template <typename A, typename B>
using is_same_or_derived = std::is_base_of<A, remove_reference_t<B>>;

template <typename A, typename B>
using disable_if_same_or_derived = disable_if<is_same_or_derived<A, B>::value>;

template <typename A, typename B>
using disable_if_same_or_derived_t =
  typename disable_if_same_or_derived<A, B>::type;

} // namespace util
} // namespace broker

#endif // BROKER_UTIL_META_HH
