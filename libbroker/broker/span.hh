// Pretty much a carbon copy of
// https://github.com/zeek/zeek/blob/v4.1.1/src/Span.h with slight modifications
// and reformatting.

#pragma once

#include <array>
#include <cstddef>
#include <iterator>
#include <type_traits>

namespace broker {

/**
 * Drop-in replacement for C++20's @c std::span with dynamic extent only:
 * https://en.cppreference.com/w/cpp/container/span. After upgrading to C++20,
 * this class may get replaced with a type alias instead and/or deprecated.
 */
template <class T>
class span {
public:
  // -- member types ---------------------------------------------------------

  using element_type = T;

  using value_type = typename std::remove_cv<T>::type;

  using index_type = size_t;

  using difference_type = ptrdiff_t;

  using pointer = T*;

  using const_pointer = const T*;

  using reference = T&;

  using const_reference = T&;

  using iterator = pointer;

  using const_iterator = const_pointer;

  using reverse_iterator = std::reverse_iterator<iterator>;

  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  // -- constructors, destructors, and assignment operators ------------------

  constexpr span() noexcept : memory_block(nullptr), num_elements(0) {}

  constexpr span(pointer ptr, size_t size)
    : memory_block(ptr), num_elements(size) {}

  constexpr span(pointer first, pointer last)
    : memory_block(first), num_elements(static_cast<size_t>(last - first)) {}

  template <size_t Size>
  constexpr span(element_type (&arr)[Size]) noexcept
    : memory_block(arr), num_elements(Size) {}

  template <class Container, class Data = typename Container::value_type,
            class = std::enable_if_t<std::is_convertible_v<Data*, T*>>>
  span(Container& xs) noexcept
    : memory_block(xs.data()), num_elements(xs.size()) {}

  template <class Container, class Data = typename Container::value_type,
            class = std::enable_if_t<std::is_convertible_v<const Data*, T*>>>
  span(const Container& xs) noexcept
    : memory_block(xs.data()), num_elements(xs.size()) {}

  constexpr span(const span&) noexcept = default;

  span& operator=(const span&) noexcept = default;

  // -- iterators ------------------------------------------------------------

  constexpr iterator begin() const noexcept {
    return memory_block;
  }

  constexpr const_iterator cbegin() const noexcept {
    return memory_block;
  }

  constexpr iterator end() const noexcept {
    return begin() + num_elements;
  }

  constexpr const_iterator cend() const noexcept {
    return cbegin() + num_elements;
  }

  constexpr reverse_iterator rbegin() const noexcept {
    return reverse_iterator{end()};
  }

  constexpr const_reverse_iterator crbegin() const noexcept {
    return const_reverse_iterator{end()};
  }

  constexpr reverse_iterator rend() const noexcept {
    return reverse_iterator{begin()};
  }

  constexpr const_reverse_iterator crend() const noexcept {
    return const_reverse_iterator{begin()};
  }

  // -- element access -------------------------------------------------------

  constexpr reference operator[](size_t index) const noexcept {
    return memory_block[index];
  }

  constexpr reference front() const noexcept {
    return *memory_block;
  }

  constexpr reference back() const noexcept {
    return (*this)[num_elements - 1];
  }

  // -- properties -----------------------------------------------------------

  constexpr size_t size() const noexcept {
    return num_elements;
  }

  constexpr size_t size_bytes() const noexcept {
    return num_elements * sizeof(element_type);
  }

  constexpr bool empty() const noexcept {
    return num_elements == 0;
  }

  constexpr pointer data() const noexcept {
    return memory_block;
  }

  // -- subviews -------------------------------------------------------------

  constexpr span subspan(size_t offset, size_t num_elements) const {
    return {memory_block + offset, num_elements};
  }

  constexpr span subspan(size_t offset) const {
    return {memory_block + offset, num_elements - offset};
  }

  constexpr span first(size_t num_elements) const {
    return {memory_block, num_elements};
  }

  constexpr span last(size_t num_elements) const {
    return subspan(num_elements - num_elements, num_elements);
  }

private:
  // -- member variables -----------------------------------------------------

  /// Points to the first element in the contiguous memory block.
  pointer memory_block;

  /// Stores the number of elements in the contiguous memory block.
  size_t num_elements;
};

// -- deduction guides ---------------------------------------------------------

template <class T>
span(T*, size_t) -> span<T>;

template <class Iter>
span(Iter, Iter) -> span<typename std::iterator_traits<Iter>::value_type>;

template <class T, size_t N>
span(T (&)[N]) -> span<T>;

template <class Container>
span(Container&) -> span<typename Container::value_type>;

template <class Container>
span(const Container&) -> span<const typename Container::value_type>;

} // namespace broker
