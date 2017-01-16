#ifndef BROKER_RESULT_HH
#define BROKER_RESULT_HH

#include "broker/status.hh"

#include "broker/detail/assert.hh"
#include "broker/detail/type_traits.hh"

namespace broker {

/// Represents the result of a computation which can either complete
/// successfully with an instance of type `T` or fail with a ::status.
/// @tparam T The type of the result.
template <class T>
class result {
public:
  // -- member types -----------------------------------------------------------

  using value_type = T;

  // -- static member variables ------------------------------------------------

  /// Stores whether move construct and move assign never throw.
  static constexpr bool nothrow_move
    = std::is_nothrow_move_constructible<T>::value
      && std::is_nothrow_move_assignable<T>::value;

  /// Stores whether copy construct and copy assign never throw.
  static constexpr bool nothrow_copy
    = std::is_nothrow_copy_constructible<T>::value
      && std::is_nothrow_copy_assignable<T>::value;

  // -- constructors, destructors, and assignment operators --------------------

  template <class U>
  result(U x, detail::enable_if_t<
                !detail::is_same_or_derived<result<T>, U>::value
                && std::is_convertible<U, T>::value>* = nullptr)
      : engaged_{true} {
    new (&value_) T(std::move(x));
  }

  result(T&& x) noexcept(nothrow_move) : engaged_{true} {
    new (&value_) T(std::move(x));
  }

  result(const T& x) noexcept(nothrow_copy) : engaged_{true} {
    new (&value_) T(x);
  }

  result(broker::status s) noexcept : engaged_{false} {
    new (&status_) broker::status{std::move(s)};
  }

  result(const result& other) noexcept(nothrow_copy) {
    construct(other);
  }

  result(sc code) : engaged_{false} {
    new (&status_) broker::status{code};
  }

  result(result&& other) noexcept(nothrow_move) {
    construct(std::move(other));
  }

  ~result() {
    destroy();
  }

  result& operator=(const result& other) noexcept(nothrow_copy) {
    if (engaged_ && other.engaged_)
      value_ = other.value_;
    else if (!engaged_ && !other.engaged_)
      status_ = other.status_;
    else {
      destroy();
      construct(other);
    }
    return *this;
  }

  result& operator=(result&& other) noexcept(nothrow_move) {
    if (engaged_ && other.engaged_)
      value_ = std::move(other.value_);
    else if (!engaged_ && !other.engaged_)
      status_ = std::move(other.status_);
    else {
      destroy();
      construct(std::move(other));
    }
    return *this;
  }

  result& operator=(const T& x) noexcept(nothrow_copy) {
    if (engaged_) {
      value_ = x;
    } else {
      destroy();
      engaged_ = true;
      new (&value_) T(x);
    }
    return *this;
  }


  result& operator=(T&& x) noexcept(nothrow_move) {
    if (engaged_) {
      value_ = std::move(x);
    } else {
      destroy();
      engaged_ = true;
      new (&value_) T(std::move(x));
    }
    return *this;
  }

  template <class U>
  detail::enable_if_t<std::is_convertible<U, T>::value, result&>
  operator=(U x) {
    return *this = T{std::move(x)};
  }

  result& operator=(broker::status s) noexcept {
    if (!engaged_)
      status_ = std::move(s);
    else {
      destroy();
      engaged_ = false;
      new (&value_) broker::status(std::move(s));
    }
    return *this;
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

  /// @copydoc cstatus
  broker::status& status() noexcept {
    BROKER_ASSERT(!engaged_);
    return status_;
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

  /// Returns the contained status.
  /// @pre `!engaged()`.
  const broker::status& cstatus() const noexcept {
    BROKER_ASSERT(!engaged_);
    return status_;
  }

  /// @copydoc cstatus
  const broker::status& status() const noexcept {
    BROKER_ASSERT(!engaged_);
    return status_;
  }

private:
  void construct(result&& other) noexcept(nothrow_move) {
    if (other.engaged_)
      new (&value_) T(std::move(other.value_));
    else
      new (&status_) broker::status(std::move(other.status_));
    engaged_ = other.engaged_;
  }

  void construct(const result& other) noexcept(nothrow_copy) {
    if (other.engaged_)
      new (&value_) T(other.value_);
    else
      new (&status_) broker::status(other.status_);
    engaged_ = other.engaged_;
  }

  void destroy() {
    if (engaged_)
      value_.~T();
    else
      status_.~status();
  }

  bool engaged_;

  union {
    T value_;
    broker::status status_;
  };
};

/// @relates result
template <class T>
auto operator==(const result<T>& x, const result<T>& y)
-> decltype(*x == *y) {
  return x && y ? *x == *y : (!x && !y ? x.status() == y.status() : false);
}

/// @relates result
template <class T, class U>
auto operator==(const result<T>& x, const U& y) -> decltype(*x == y) {
  return x ? *x == y : false;
}

/// @relates result
template <class T, class U>
auto operator==(const T& x, const result<U>& y) -> decltype(x == *y) {
  return y == x;
}

/// @relates result
template <class T>
bool operator==(const result<T>& x, const status& y) {
  return x ? false : x.status() == y;
}

/// @relates result
template <class T>
bool operator==(const status& x, const result<T>& y) {
  return y == x;
}

/// @relates result
template <class T>
bool operator==(const result<T>& x, sc y) {
  return !x && x.status().code() == y;
}

/// @relates result
template <class T>
bool operator==(sc x, const result<T>& y) {
  return y == x;
}

/// @relates result
template <class T>
auto operator!=(const result<T>& x, const result<T>& y) -> decltype(*x == *y) {
  return !(x == y);
}

/// @relates result
template <class T, class U>
auto operator!=(const result<T>& x, const U& y) -> decltype(*x == y) {
  return !(x == y);
}

/// @relates result
template <class T, class U>
auto operator!=(const T& x, const result<U>& y) -> decltype(x == *y) {
  return !(x == y);
}

/// @relates result
template <class T>
bool operator!=(const result<T>& x, const status& y) {
  return !(x == y);
}

/// @relates result
template <class T>
bool operator!=(const status& x, const result<T>& y) {
  return !(x == y);
}

/// @relates result
template <class T>
bool operator!=(const result<T>& x, sc y) {
  return !(x == y);
}

/// @relates result
template <class T>
bool operator!=(sc x, const result<T>& y) {
  return y != x;
}

/// The pattern `result<void>` shall be used for functions that may generate
/// a status but would otherwise return `bool`.
template <>
class result<void> {
public:
  result() = default;

  result(broker::status s) noexcept : status_(std::move(s)) {
    // nop
  }

  result(const result& other)  noexcept : status_(other.status_) {
    // nop
  }

  result(result&& other) noexcept : status_(std::move(other.status_)) {
    // nop
  }

  result(sc code) : status_{code} {
    // nop
  }

  result& operator=(const result& other) = default;

  result& operator=(result&& other) noexcept {
    status_ = std::move(other.status_);
    return *this;
  }

  explicit operator bool() const {
    // We have good result only when the status is default-constructed,
    // which means the internal error is "none".
    return status_.error_ == caf::none;
  }

  const broker::status& status() const {
    return status_;
  }

private:
  broker::status status_;
};

/// @relates result
inline bool operator==(const result<void>& x, const result<void>& y) {
  return (x && y) || (!x && !y && x.status() == y.status());
}

/// @relates result
inline bool operator!=(const result<void>& x, const result<void>& y) {
  return !(x == y);
}

template <class T>
auto to_string(const result<T>& x) -> decltype(to_string(*x)) {
  if (x)
    return to_string(*x);
  return "!" + to_string(x.status());
}

} // namespace broker

#endif // BROKER_RESULT_HH
