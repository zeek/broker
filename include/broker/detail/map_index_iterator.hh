#pragma once

#include <iterator>
#include <tuple>
#include <utility>

namespace broker::detail {

/// Maps an iterator over `std::pair` or `std::tuple` elements to an iterator
/// over the `N`th type in each element. For example, iterating over a range of
/// `std::pair<int, double>` elements with `N = 0` would only visit the `int`
/// portion of each element.
template <class Iterator, size_t N>
class map_index_iterator {
public:
  using base_trait = std::iterator_traits<Iterator>;

  static constexpr bool is_const = std::is_const<
    std::remove_reference_t<typename base_trait::reference>>::value;

  using value_type = std::tuple_element_t<N, typename base_trait::value_type>;

  using pointer = std::conditional_t<is_const, const value_type*, value_type*>;

  using reference
    = std::conditional_t<is_const, const value_type&, value_type&>;

  using difference_type = typename base_trait::difference_type;

  using iterator_category = typename base_trait::iterator_category;

  static constexpr bool is_random_access
    = std::is_same<iterator_category, std::random_access_iterator_tag>::value;

  explicit map_index_iterator(Iterator iter) : iter_(iter) {
    // nop
  }

  map_index_iterator(const map_index_iterator&) = default;

  map_index_iterator& operator=(const map_index_iterator&) = default;

  // -- proerties --------------------------------------------------------------

  auto base() const {
    return iter_;
  }

  // -- operators --------------------------------------------------------------

  reference operator*() {
    return std::get<N>(*iter_);
  }

  map_index_iterator& operator++() {
    ++iter_;
    return *this;
  }

  map_index_iterator operator++(int) {
    return map_index_iterator{iter_++};
  }

  map_index_iterator& operator--() {
    --iter_;
    return *this;
  }

  map_index_iterator operator--(int) {
    return map_index_iterator{iter_--};
  }

  // -- conditional operators --------------------------------------------------

  template <class T = map_index_iterator&>
  std::enable_if_t<is_random_access, T> operator-=(difference_type n) {
    iter_ -= n;
    return *this;
  }

  template <class T = map_index_iterator&>
  std::enable_if_t<is_random_access, T> operator+=(difference_type n) {
    iter_ += n;
    return *this;
  }

  template <class T = reference>
  std::enable_if_t<is_random_access, T> operator[](size_t index) {
    return std::get<N>(iter_[index]);
  }


private:
  Iterator iter_;
};

template <class Iterator>
using map_first_iterator = map_index_iterator<Iterator, 0>;

template <class Iterator>
using map_second_iterator = map_index_iterator<Iterator, 1>;

template <class Iterator>
auto map_first(Iterator iter) {
  return map_index_iterator<Iterator, 0>(iter);
}

template <class Iterator>
auto map_second(Iterator iter) {
  return map_index_iterator<Iterator, 1>(iter);
}

// -- free operators -----------------------------------------------------------

template <class Iterator, size_t N>
bool operator==(map_index_iterator<Iterator, N> x,
                map_index_iterator<Iterator, N> y) {
  return x.base() == y.base();
}

template <class Iterator, size_t N>
bool operator!=(map_index_iterator<Iterator, N> x,
                map_index_iterator<Iterator, N> y) {
  return x.base() != y.base();
}

// -- conditional free operators -----------------------------------------------

template <class Iterator, size_t N>
std::enable_if_t<map_index_iterator<Iterator, N>::is_random_access,
                 map_index_iterator<Iterator, N>>
operator+(map_index_iterator<Iterator, N> x,
          typename map_index_iterator<Iterator, N>::difference_type n) {
  auto result = x;
  result += n;
  return result;
}

template <class Iterator, size_t N>
std::enable_if_t<map_index_iterator<Iterator, N>::is_random_access,
                 map_index_iterator<Iterator, N>>
operator+(typename map_index_iterator<Iterator, N>::difference_type n,
          map_index_iterator<Iterator, N> x) {
  auto result = x;
  result += n;
  return result;
}

template <class Iterator, size_t N>
std::enable_if_t<map_index_iterator<Iterator, N>::is_random_access,
                 map_index_iterator<Iterator, N>>
operator-(map_index_iterator<Iterator, N> x,
          typename map_index_iterator<Iterator, N>::difference_type n) {
  auto result = x;
  result -= n;
  return result;
}

template <class Iterator, size_t N>
auto operator-(map_index_iterator<Iterator, N> x,
               map_index_iterator<Iterator, N> y)
  -> decltype(x.base() - y.base()) {
  return x.base() - y.base();
}

} // namespace broker::detail
