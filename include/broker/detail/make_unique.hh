#ifndef BROKER_DETAIL_MAKE_UNIQUE_HH
#define BROKER_DETAIL_MAKE_UNIQUE_HH

#include <memory>

namespace std {

// C++11 backport of std::make_unique.
template <class T, class... Ts>
unique_ptr<T> make_unique(Ts&&... xs) {
  return unique_ptr<T>(new T(std::forward<Ts>(xs)...));
}

} // namespace std

#endif // BROKER_DETAIL_MAKE_UNIQUE_HH
