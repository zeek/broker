#pragma once

#include "broker/detail/type_traits.hh"
#include "broker/envelope.hh"
#include "broker/variant.hh"
#include "broker/variant_data.hh"

#include <iterator>

namespace broker {

/// A list of @ref variant values.
class variant_list {
public:
  // -- friend types -----------------------------------------------------------

  friend class variant;

  // -- member types -----------------------------------------------------------

  class iterator {
  public:
    using iterator_category = std::forward_iterator_tag;

    using value_type = variant;

    using difference_type = ptrdiff_t;

    friend class variant_list;

    iterator() noexcept = default;

    iterator(const iterator&) noexcept = default;

    iterator& operator=(const iterator&) noexcept = default;

    iterator& operator++() noexcept {
      ++pos_;
      return *this;
    }

    iterator operator++(int) noexcept {
      auto tmp = *this;
      ++pos_;
      return tmp;
    }

    variant value() const noexcept {
      return variant{std::addressof(*pos_), shared_envelope()};
    }

    variant operator*() const noexcept {
      return value();
    }

    variant operator->() const noexcept {
      return value();
    }

    friend bool operator==(const iterator& lhs, const iterator& rhs) noexcept {
      return lhs.pos_ == rhs.pos_;
    }

    friend bool operator!=(const iterator& lhs, const iterator& rhs) noexcept {
      return lhs.pos_ != rhs.pos_;
    }

  private:
    using native_iterator = variant_data::list_iterator;

    data_envelope_ptr shared_envelope() const {
      return {new_ref, envelope_};
    }

    iterator(native_iterator pos, const data_envelope* ptr) noexcept
      : pos_(pos), envelope_(ptr) {
      // nop
    }

    native_iterator pos_;
    const data_envelope* envelope_;
  };

  // -- constructors, destructors, and assignment operators --------------------

  variant_list() noexcept = default;

  variant_list(variant_list&&) noexcept = default;

  variant_list(const variant_list&) noexcept = default;

  bool empty() const noexcept {
    return values_ ? values_->empty() : true;
  }

  size_t size() const noexcept {
    return values_ ? values_->size() : 0u;
  }

  iterator begin() const noexcept {
    return iterator{values_ ? values_->begin() : iterator::native_iterator{},
                    envelope_.get()};
  }

  iterator end() const noexcept {
    return iterator{values_ ? values_->end() : iterator::native_iterator{},
                    envelope_.get()};
  }

  // -- element access ---------------------------------------------------------

  // TODO: this is only implemented for compatibility with broker::vector API
  //       and to have existing algorithms work with variant. Should be
  //       removed eventually, because it is very inefficient.
  variant operator[](size_t index) const noexcept {
    auto i = values_->begin();
    std::advance(i, index);
    return variant{std::addressof(*i), envelope_};
  }

  variant at(size_t index) const {
    if (index < size())
      return (*this)[index];
    return variant{};
  }

  variant front() const noexcept {
    return variant{std::addressof(values_->front()), envelope_};
  }

  variant back() const noexcept {
    return variant{std::addressof(values_->back()), envelope_};
  }

  /// Returns the first `N` elements of the list as an array. If the list has
  /// less than `N` elements, the resulting array will be filled with
  /// default-constructed variants, i.e., `nil` values.
  template <size_t N>
  std::array<variant, N> take() const noexcept {
    std::array<variant, N> result;
    auto i = values_->begin();
    auto e = values_->end();
    auto j = result.begin();
    for (size_t n = 0; n < N; ++n) {
      if (i == e)
        break;
      *j++ = variant{std::addressof(*i++), envelope_};
    }
    return result;
  }

  // -- properties -------------------------------------------------------------

  /// Returns a raw pointer to the managed object.
  const auto* raw() const noexcept {
    return values_;
  }

  // -- conversions ------------------------------------------------------------

  data to_data() const;

private:
  variant_list(const variant_data::list* values, data_envelope_ptr ptr) noexcept
    : values_(values), envelope_(std::move(ptr)) {
    // nop
  }

  /// The list of values.
  const variant_data::list* values_ = nullptr;

  /// The envelope that holds the data.
  data_envelope_ptr envelope_;
};

/// @relates variant_list
bool operator==(const variant_list& lhs, const variant_list& rhs) noexcept;

/// @relates variant_list
bool operator==(const variant_list& lhs, const vector& rhs) noexcept;

/// @relates variant_list
inline bool operator==(const vector& lhs, const variant_list& rhs) noexcept {
  return rhs == lhs;
}

/// @relates variant_list
inline bool operator!=(const variant_list& lhs,
                       const variant_list& rhs) noexcept {
  return !(lhs == rhs);
}

/// @relates variant_list
inline bool operator!=(const variant_list& lhs, const vector& rhs) noexcept {
  return !(lhs == rhs);
}

/// @relates variant_list
inline bool operator!=(const vector& lhs, const variant_list& rhs) noexcept {
  return !(lhs == rhs);
}

/// End of recursion for `contains`.
inline bool contains_impl(variant_list::iterator, detail::parameter_pack<>) {
  return true;
}

/// Recursively checks whether the types in `Ts` match the types in `pos`.
template <class T, class... Ts>
bool contains_impl(variant_list::iterator pos,
                   detail::parameter_pack<T, Ts...>) {
  if (!exact_match_or_can_convert_to<T>(*pos++))
    return false;
  return contains_impl(pos, detail::parameter_pack<Ts...>{});
}

/// Checks whether `xs` contains values of types `Ts...`. Performs "fuzzy"
/// matching by calling `can_convert_to<T>` for any `T` that is not part of the
/// variant.
template <class... Ts>
bool contains(const variant_list& xs) {
  if (xs.size() != sizeof...(Ts))
    return false;
  return contains_impl(xs.begin(), detail::parameter_pack<Ts...>{});
}

/// Converts `what` to a string.
void convert(const variant_list& what, std::string& out);

/// Converts `what` to a string.
void convert(const variant_list& what, vector& out);

/// Prints `what` to `out`.
std::ostream& operator<<(std::ostream& out, const variant_list& what);

} // namespace broker
