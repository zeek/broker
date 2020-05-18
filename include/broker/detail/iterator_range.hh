#pragma once

#include <algorithm>
#include <iterator>

namespace broker::detail {

/// A lightweight range abstraction using a pair of iterators.
template <class Iterator>
class iterator_range {
public:
  using trait = std::iterator_traits<Iterator>;

  using iterator = Iterator;

  using value_type = typename trait::value_type;

  iterator_range(iterator first, iterator last) : begin_(first), end_(last) {
    // nop
  }

  iterator_range(const iterator_range&) = default;

  iterator_range& operator=(const iterator_range&) = default;

  iterator begin() const {
    return begin_;
  }

  iterator end() const {
    return end_;
  }

  bool empty() const {
    return begin_ == end_;
  }

private:
  iterator begin_;
  iterator end_;
};

/// Convenience function for creating an iterator range from a pair of
/// iterators.
/// @relates iterator_range
template <class Iterator>
auto make_iterator_range(Iterator begin, Iterator end) {
  return iterator_range<Iterator>(begin, end);
}

/// Convenience function for creating an iterator range from a pair of
/// iterators.
/// @relates iterator_range
template <class Container>
auto make_iterator_range(const Container& xs) {
  return make_iterator_range(xs.begin(), xs.end());
}

/// @relates iterator_range
template <class Iterator1, class Iterator2>
bool operator==(iterator_range<Iterator1> xs, iterator_range<Iterator2> ys) {
  return std::equal(xs.begin(), xs.end(), ys.begin(), ys.end());
}

/// @relates iterator_range
template <class Iterator, class Container>
bool operator==(iterator_range<Iterator> xs, const Container& ys) {
  return xs == make_iterator_range(ys);
}

/// @relates iterator_range
template <class Container, class Iterator>
bool operator==(const Container& xs, iterator_range<Iterator> ys) {
  return make_iterator_range(xs) == ys;
}

/// @relates iterator_range
template <class Iterator1, class Iterator2>
bool operator!=(iterator_range<Iterator1> xs, iterator_range<Iterator2> ys) {
  return !(xs == ys);
}

/// @relates iterator_range
template <class Iterator, class Container>
bool operator!=(iterator_range<Iterator> xs, const Container& ys) {
  return !(xs == ys);
}

/// @relates iterator_range
template <class Container, class Iterator>
bool operator!=(const Container& xs, iterator_range<Iterator> ys) {
  return !(xs == ys);
}

} // namespace broker::detail
