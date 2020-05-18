#pragma once

#include <cstddef>

namespace broker::detail {

// TODO: switch to the new std::erase_if overloads when switching to C++20.
template <class Container, class Predicate>
size_t erase_if(Container& xs, Predicate predicate) {
  size_t old_size = xs.size();
  auto i = xs.begin();
  while (i != xs.end())
    if (predicate(*i))
      i = xs.erase(i);
    else
      ++i;
  return old_size - xs.size();
}

} // namespace broker::detail
