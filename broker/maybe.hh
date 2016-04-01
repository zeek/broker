#ifndef BROKER_MAYBE_HH
#define BROKER_MAYBE_HH

#include <caf/maybe.hpp>

namespace broker {

using caf::maybe;

// As in std::experimental::optional.

template <class T>
bool operator<(const maybe<T>& lhs, const maybe<T>& rhs) {
  if (!rhs)
    return false;
  if (!lhs)
    return true;
  return *lhs < *rhs;
}

template <class T>
bool operator<=(const maybe<T>& lhs, const maybe<T>& rhs) {
  return !(rhs < lhs);
}

template <class T>
bool operator>=(const maybe<T>& lhs, const maybe<T>& rhs) {
  return !(lhs < rhs);
}

template <class T>
bool operator>(const maybe<T>& lhs, const maybe<T>& rhs) {
  return rhs < lhs;
}

template <class T>
bool operator<(const maybe<T>& lhs, const T& rhs) {
  return (lhs) ? *lhs < rhs : true;
}

template <class T>
bool operator<(const T& lhs, const maybe<T>& rhs) {
  return (rhs) ? rhs < *lhs : false;
}

template <class T>
bool operator<=(const maybe<T>& lhs, const T& rhs) {
  return !(lhs > rhs);
}

template <class T>
bool operator<=(const T& lhs, const maybe<T>& rhs) {
  return !(lhs > rhs);
}

template <class T>
bool operator>(const maybe<T>& lhs, const T& rhs) {
  return (lhs) ? lhs < rhs : false;
}

template <class T>
bool operator>(const T& lhs, const maybe<T>& rhs) {
  return (rhs) ? rhs < lhs : true;
}

template <class T>
bool operator>=(const maybe<T>& lhs, const T& rhs) {
  return !(lhs < rhs);
}

template <class T>
bool operator>=(const T& lhs, const maybe<T>& rhs) {
  return !(rhs < lhs);
}

} // namespace broker

namespace std {

template <class T>
struct hash<broker::maybe<T>> {
  using result_type = typename hash<T>::result_type;
  using argument_type = broker::maybe<T>;

  inline result_type operator()(const argument_type& arg) const {
    if (arg)
      return std::hash<T>{}(*arg);
    return result_type{};
  }
};

template <class T>
struct hash<broker::maybe<T&>> {
  using result_type = typename hash<T>::result_type;
  using argument_type = broker::maybe<T&>;

  inline result_type operator()(const argument_type& arg) const {
    if (arg)
      return std::hash<T>{}(*arg);
    return result_type{};
  }
};

} // namespace std

#endif // BROKER_MAYBE_HH
