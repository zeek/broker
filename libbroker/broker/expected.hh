// This implementation is based on caf::expected. Original implementation see:
// https://github.com/actor-framework/actor-framework/blob/0.18.5/libcaf_core/caf/expected.hpp.
//
// -- Original header ----------------------------------------------------------
// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.
// -----------------------------------------------------------------------------

#pragma once

#include "broker/detail/assert.hh"
#include "broker/error.hh"

#include <type_traits>

namespace broker {

/// Represents the result of a computation which can either complete
/// successfully with an instance of type `T` or fail with an `error`.
/// @tparam T The type of the result.
template <typename T>
class expected {
public:
  // -- member types -----------------------------------------------------------

  using value_type = T;

  // -- static member variables ------------------------------------------------

  /// Stores whether move construct and move assign never throw.
  static constexpr bool nothrow_move =
    std::is_nothrow_move_constructible_v<T> //
    && std::is_nothrow_move_assignable_v<T>;

  /// Stores whether copy construct and copy assign never throw.
  static constexpr bool nothrow_copy =
    std::is_nothrow_copy_constructible_v<T> //
    && std::is_nothrow_copy_assignable_v<T>;

  // -- constructors, destructors, and assignment operators --------------------

  template <class U>
  expected(U x, std::enable_if_t<std::is_convertible_v<U, T>>* = nullptr)
    : engaged_(true) {
    new (std::addressof(value_)) T(std::move(x));
  }

  expected(T&& x) noexcept(nothrow_move) : engaged_(true) {
    new (std::addressof(value_)) T(std::move(x));
  }

  expected(const T& x) noexcept(nothrow_copy) : engaged_(true) {
    new (std::addressof(value_)) T(x);
  }

  expected(broker::error e) noexcept : engaged_(false) {
    new (std::addressof(error_)) broker::error{std::move(e)};
  }

  expected(const expected& other) noexcept(nothrow_copy) {
    construct(other);
  }

  expected(ec code) : engaged_(false) {
    new (std::addressof(error_)) broker::error(code);
  }

  expected(expected&& other) noexcept(nothrow_move) {
    construct(std::move(other));
  }

  ~expected() {
    destroy();
  }

  expected& operator=(const expected& other) noexcept(nothrow_copy) {
    if (engaged_ && other.engaged_)
      value_ = other.value_;
    else if (!engaged_ && !other.engaged_)
      error_ = other.error_;
    else {
      destroy();
      construct(other);
    }
    return *this;
  }

  expected& operator=(expected&& other) noexcept(nothrow_move) {
    if (engaged_ && other.engaged_)
      value_ = std::move(other.value_);
    else if (!engaged_ && !other.engaged_)
      error_ = std::move(other.error_);
    else {
      destroy();
      construct(std::move(other));
    }
    return *this;
  }

  expected& operator=(const T& x) noexcept(nothrow_copy) {
    if (engaged_) {
      value_ = x;
    } else {
      destroy();
      engaged_ = true;
      new (std::addressof(value_)) T(x);
    }
    return *this;
  }

  expected& operator=(T&& x) noexcept(nothrow_move) {
    if (engaged_) {
      value_ = std::move(x);
    } else {
      destroy();
      engaged_ = true;
      new (std::addressof(value_)) T(std::move(x));
    }
    return *this;
  }

  template <class U>
  std::enable_if_t<std::is_convertible_v<U, T>, expected&> operator=(U x) {
    return *this = T{std::move(x)};
  }

  expected& operator=(broker::error e) noexcept {
    if (!engaged_)
      error_ = std::move(e);
    else {
      destroy();
      engaged_ = false;
      new (std::addressof(error_)) broker::error(std::move(e));
    }
    return *this;
  }

  expected& operator=(ec code) {
    return *this = make_error(code);
  }

  // -- modifiers --------------------------------------------------------------

  /// @copydoc cvalue
  T& value() noexcept {
    BROKER_ASSERT(engaged_);
    return value_;
  }

  /// @copydoc cvalue
  T& operator*() noexcept {
    return value();
  }

  /// @copydoc cvalue
  T* operator->() noexcept {
    return &value();
  }

  /// @copydoc cerror
  broker::error& error() noexcept {
    BROKER_ASSERT(!engaged_);
    return error_;
  }

  // -- observers --------------------------------------------------------------

  /// Returns the contained value.
  /// @pre `engaged() == true`.
  const T& cvalue() const noexcept {
    BROKER_ASSERT(engaged_);
    return value_;
  }

  /// @copydoc cvalue
  const T& value() const noexcept {
    BROKER_ASSERT(engaged_);
    return value_;
  }

  /// @copydoc cvalue
  const T& operator*() const noexcept {
    return value();
  }

  /// @copydoc cvalue
  const T* operator->() const noexcept {
    return &value();
  }

  /// @copydoc engaged
  explicit operator bool() const noexcept {
    return engaged();
  }

  /// Returns `true` if the object holds a value (is engaged).
  bool engaged() const noexcept {
    return engaged_;
  }

  /// Returns the contained error.
  /// @pre `engaged() == false`.
  const broker::error& cerror() const noexcept {
    BROKER_ASSERT(!engaged_);
    return error_;
  }

  /// @copydoc cerror
  const broker::error& error() const noexcept {
    BROKER_ASSERT(!engaged_);
    return error_;
  }

private:
  void construct(expected&& other) noexcept(nothrow_move) {
    if (other.engaged_)
      new (std::addressof(value_)) T(std::move(other.value_));
    else
      new (std::addressof(error_)) broker::error(std::move(other.error_));
    engaged_ = other.engaged_;
  }

  void construct(const expected& other) noexcept(nothrow_copy) {
    if (other.engaged_)
      new (std::addressof(value_)) T(other.value_);
    else
      new (std::addressof(error_)) broker::error(other.error_);
    engaged_ = other.engaged_;
  }

  void destroy() {
    if (engaged_)
      value_.~T();
    else
      error_.~error();
  }

  bool engaged_;

  union {
    T value_;
    broker::error error_;
  };
};

/// @relates expected
template <class T>
auto operator==(const expected<T>& x, const expected<T>& y)
  -> decltype(*x == *y) {
  return x && y ? *x == *y : (!x && !y ? x.error() == y.error() : false);
}

/// @relates expected
template <class T, class U>
auto operator==(const expected<T>& x, const U& y) -> decltype(*x == y) {
  return x ? *x == y : false;
}

/// @relates expected
template <class T, class U>
auto operator==(const T& x, const expected<U>& y) -> decltype(x == *y) {
  return y == x;
}

/// @relates expected
template <class T>
bool operator==(const expected<T>& x, const error& y) {
  return x ? false : x.error() == y;
}

/// @relates expected
template <class T>
bool operator==(const error& x, const expected<T>& y) {
  return y == x;
}

/// @relates expected
template <class T>
bool operator==(const expected<T>& x, ec code) {
  return x ? false
           : x.error().compare(static_cast<uint8_t>(code), ec_category()) == 0;
}

/// @relates expected
template <class T>
bool operator==(ec x, const expected<T>& y) {
  return y == x;
}

/// @relates expected
template <class T>
auto operator!=(const expected<T>& x, const expected<T>& y)
  -> decltype(*x == *y) {
  return !(x == y);
}

/// @relates expected
template <class T, class U>
auto operator!=(const expected<T>& x, const U& y) -> decltype(*x == y) {
  return !(x == y);
}

/// @relates expected
template <class T, class U>
auto operator!=(const T& x, const expected<U>& y) -> decltype(x == *y) {
  return !(x == y);
}

/// @relates expected
template <class T>
bool operator!=(const expected<T>& x, const error& y) {
  return !(x == y);
}

/// @relates expected
template <class T>
bool operator!=(const error& x, const expected<T>& y) {
  return !(x == y);
}

/// @relates expected
template <class T>
bool operator!=(const expected<T>& x, ec y) {
  return !(x == y);
}

/// @relates expected
template <class T>
bool operator!=(ec x, const expected<T>& y) {
  return !(x == y);
}

/// The pattern `expected<void>` shall be used for functions that may generate
/// an error but would otherwise return `bool`.
template <>
class expected<void> {
public:
  expected() = default;

  expected(broker::error e) noexcept : error_(std::move(e)) {
    // nop
  }

  expected(const expected& other) noexcept : error_(other.error_) {
    // nop
  }

  expected(expected&& other) noexcept : error_(std::move(other.error_)) {
    // nop
  }

  expected(ec code) : error_(code) {
    // nop
  }

  expected& operator=(const expected& other) = default;

  expected& operator=(expected&& other) noexcept {
    error_ = std::move(other.error_);
    return *this;
  }

  explicit operator bool() const {
    return !error_;
  }

  const broker::error& error() const {
    return error_;
  }

private:
  broker::error error_;
};

/// @relates expected
inline bool operator==(const expected<void>& x, const expected<void>& y) {
  return (x && y) || (!x && !y && x.error() == y.error());
}

/// @relates expected
inline bool operator!=(const expected<void>& x, const expected<void>& y) {
  return !(x == y);
}

inline std::string to_string(const expected<void>& x) {
  if (x)
    return "unit";
  else
    return "!" + to_string(x.error());
}

} // namespace broker
