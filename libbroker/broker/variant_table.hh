#pragma once

#include "broker/variant.hh"
#include "broker/variant_data.hh"

namespace broker {

/// A view into a map of data objects.
class variant_table {
public:
  // -- friend types -----------------------------------------------------------

  friend class variant;

  // -- member types -----------------------------------------------------------

  struct key_value_pair {
    variant first;
    variant second;

    /// Returns a pointer to the underlying data.
    const key_value_pair* operator->() const noexcept {
      // Note: this is only implemented for "drill-down" access from iterators.
      return this;
    }
  };

  class iterator {
  public:
    friend class variant_table;

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

    variant key() const noexcept {
      return variant{std::addressof(pos_->first), shared_envelope()};
    }

    variant value() const noexcept {
      return variant{std::addressof(pos_->second), shared_envelope()};
    }

    key_value_pair operator*() const noexcept {
      return {key(), value()};
    }

    key_value_pair operator->() const noexcept {
      return {key(), value()};
    }

    friend bool operator==(const iterator& lhs, const iterator& rhs) noexcept {
      return lhs.pos_ == rhs.pos_;
    }

    friend bool operator!=(const iterator& lhs, const iterator& rhs) noexcept {
      return lhs.pos_ != rhs.pos_;
    }

  private:
    using native_iterator = variant_data::table_iterator;

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

  variant_table() noexcept = default;

  variant_table(variant_table&&) noexcept = default;

  variant_table(const variant_table&) noexcept = default;

  variant_table(const variant_data::table* values,
                data_envelope_ptr ptr) noexcept
    : values_(values), envelope_(std::move(ptr)) {
    // nop
  }

  variant_table& operator=(variant_table&&) noexcept = default;

  variant_table& operator=(const variant_table&) noexcept = default;

  // -- properties -------------------------------------------------------------

  bool empty() const noexcept {
    return values_ ? values_->empty() : true;
  }

  size_t size() const noexcept {
    return values_ ? values_->size() : 0u;
  }

  /// @private
  const auto* raw() const noexcept {
    return values_;
  }

  // -- iterator access --------------------------------------------------------

  iterator begin() const noexcept {
    return iterator{values_ ? values_->begin() : iterator::native_iterator{},
                    envelope_.get()};
  }

  iterator end() const noexcept {
    return iterator{values_ ? values_->end() : iterator::native_iterator{},
                    envelope_.get()};
  }

  // -- key lookup -------------------------------------------------------------

  iterator find(const variant& key) const {
    return iterator{values_->find(*key->raw()), envelope_.get()};
  }

  variant operator[](const variant& key) const {
    if (auto i = values_->find(*key.raw()); i != values_->end())
      return variant{std::addressof(i->second), envelope_};
    return variant{};
  }

  variant operator[](std::string_view key) const {
    return do_lookup(key);
  }

private:
  template <class T>
  variant do_lookup(T key) const {
    if (values_ == nullptr)
      return variant{};
    auto key_view = variant_data{key};
    if (auto i = values_->find(key_view); i != values_->end())
      return variant{std::addressof(i->second), envelope_};
    return variant{};
  }

  /// The list of values.
  const variant_data::table* values_ = nullptr;

  /// The envelope that holds the data.
  data_envelope_ptr envelope_;
};

/// Converts `what` to a string.
void convert(const variant_table& what, std::string& out);

/// Prints `what` to `out`.
std::ostream& operator<<(std::ostream& out, const variant_table& what);

} // namespace broker
