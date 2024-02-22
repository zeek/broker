#pragma once

#include "broker/detail/promote.hh"
#include "broker/envelope.hh"
#include "broker/variant.hh"
#include "broker/variant_data.hh"

namespace broker {

/// A view into a set of data objects.
class variant_set {
public:
  // -- friend types -----------------------------------------------------------

  friend class variant;

  // -- member types -----------------------------------------------------------

  class iterator {
  public:
    friend class variant_set;

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
    using native_iterator = variant_data::set_iterator;

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

  variant_set() noexcept = default;

  variant_set(const variant_set&) noexcept = default;

  variant_set(const variant_data::set* values, data_envelope_ptr ptr) noexcept
    : values_(values), envelope_(std::move(ptr)) {
    // nop
  }

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

  // -- properties -------------------------------------------------------------

  /// Grants access to the managed object.
  const auto* raw() const noexcept {
    return values_;
  }

  /// Returns a shared pointer to the @ref envelope that owns the stored data.
  auto shared_envelope() const noexcept {
    return envelope_;
  }

  /// Checks whether this set contains `what`.
  template <class T>
  bool contains(T&& what) const noexcept {
    auto&& pval = detail::promote<T>(what);
    using val_t = std::decay_t<decltype(pval)>;
    static_assert(variant_data::is_primitive<val_t>,
                  "value must be a primitive variant_data type");
    variant_data tmp{what};
    return values_->find(tmp) != values_->end();
  }

private:
  /// The list of values.
  const variant_data::set* values_ = nullptr;

  /// The envelope that holds the data.
  data_envelope_ptr envelope_;
};

/// Converts `what` to a string.
void convert(const variant_set& what, std::string& out);

/// Prints `what` to `out`.
std::ostream& operator<<(std::ostream& out, const variant_set& what);

} // namespace broker
