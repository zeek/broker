// This implementation is based on caf::cow_tuple. Original implementation see:
// https://github.com/actor-framework/actor-framework/blob/0.18.5/libcaf_core/caf/cow_tuple.hpp.
//
// -- Original header ----------------------------------------------------------
// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.
// -----------------------------------------------------------------------------

#pragma once

#include "broker/config.hh"
#include "broker/detail/comparable.hh"

#include <atomic>
#include <cstddef>
#include <tuple>

namespace broker {

/// A copy-on-write tuple implementation.
template <class... Ts>
class cow_tuple : detail::comparable<cow_tuple<Ts...>>,
                  detail::comparable<cow_tuple<Ts...>, std::tuple<Ts...>> {
public:
  // -- sanity checks ----------------------------------------------------------

  static_assert(sizeof...(Ts) > 0);

  // -- member types -----------------------------------------------------------

  using data_type = std::tuple<Ts...>;

  using ref_count_type = std::atomic<size_t>;

  struct impl {
    explicit impl(Ts&&... xs) : rc(1), data(std::move(xs)...) {
      // nop
    }

    impl() : rc(1) {
      // nop
    }

    impl(const impl& other) : rc(1), data(other.data) {
      // nop
    }

    impl& operator=(const impl& other) {
      data = other.data;
      return *this;
    }

    impl* copy() const {
      return new impl{*this};
    }

    void ref() const noexcept {
      ++rc;
    }

    void deref() const noexcept {
      if (--rc == 0)
        delete this;
    }

    bool unique() const noexcept {
      return rc.load() == 1;
    }

    alignas(BROKER_CONSTRUCTIVE_INTERFERENCE_SIZE) mutable ref_count_type rc;

    data_type data;
  };

  // -- constructors, destructors, and assignment operators --------------------

  explicit cow_tuple(Ts... xs) : ptr_(new impl(std::move(xs)...)) {
    // nop
  }

  cow_tuple() : ptr_(new impl) {
    // nop
  }

  cow_tuple(cow_tuple&& other) noexcept : ptr_(other.ptr_) {
    other.ptr_ = nullptr;
  }

  cow_tuple(const cow_tuple& other) noexcept : ptr_(other.ptr_) {
    if (ptr_)
      ptr_->ref();
  }

  cow_tuple& operator=(cow_tuple&& other) noexcept {
    if (this != &other) {
      if (ptr_)
        ptr_->deref();
      ptr_ = other.ptr_;
      other.ptr_ = nullptr;
    }
    return *this;
  }

  cow_tuple& operator=(const cow_tuple& other) noexcept {
    if (this != &other) {
      if (ptr_)
        ptr_->deref();
      ptr_ = other.ptr_;
      if (ptr_)
        ptr_->ref();
    }
    return *this;
  }

  ~cow_tuple() noexcept {
    if (ptr_)
      ptr_->deref();
  }

  // -- properties -------------------------------------------------------------

  /// Returns the managed tuple.
  const data_type& data() const noexcept {
    return ptr_->data;
  }

  /// Returns a mutable reference to the managed tuple, guaranteed to have a
  /// reference count of 1.
  data_type& unshared() {
    if (ptr_->unique()) {
      return ptr_->data;
    } else {
      auto new_ptr = ptr_->copy();
      ptr_->deref();
      ptr_ = new_ptr;
      return new_ptr->data;
    }
  }

  /// Checks whether this object holds the only reference to the data.
  bool unique() const noexcept {
    return ptr_->unique();
  }

  // -- comparison -------------------------------------------------------------

  template <class... Us>
  int compare(const std::tuple<Us...>& other) const noexcept {
    return data() < other ? -1 : (data() == other ? 0 : 1);
  }

  template <class... Us>
  int compare(const cow_tuple<Us...>& other) const noexcept {
    return compare(other.data());
  }

private:
  // -- member variables -------------------------------------------------------

  impl* ptr_;
};

/// Convenience function for calling `get<N>(xs.data())`.
/// @relates cow_tuple
template <size_t N, class... Ts>
decltype(auto) get(const cow_tuple<Ts...>& xs) {
  return std::get<N>(xs.data());
}

} // namespace broker
