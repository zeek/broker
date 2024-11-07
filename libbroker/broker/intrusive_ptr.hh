#pragma once

#include <functional>
#include <type_traits>
#include <utility>

namespace broker {

/// A tag class for the @ref intrusive_ptr constructor which means: adopt the
/// reference from the caller.
struct adopt_ref_t {};

/// A tag value of type @ref adopt_ref_t.
constexpr adopt_ref_t adopt_ref{};

/// A tag class for the @ref intrusive_ptr constructor which means: create a new
/// reference to the object.
struct new_ref_t {};

/// A tag value of type @ref new_ref_t.
constexpr new_ref_t new_ref{};

/// An intrusive, reference counting smart pointer implementation similar to
/// `zeek::IntrusivePtr`.
template <class T>
class intrusive_ptr {
public:
  // -- member types

  using pointer = T*;

  using const_pointer = const T*;

  using element_type = T;

  using reference = T&;

  using const_reference = const T&;

  // -- constructors, destructors, and assignment operators

  constexpr intrusive_ptr() noexcept = default;

  constexpr intrusive_ptr(std::nullptr_t) noexcept {}

  /// Constructs a new intrusive pointer for managing the lifetime of the object
  /// pointed to by @c raw_ptr.
  ///
  /// This overload adopts the existing reference from the caller.
  ///
  /// @param raw_ptr Pointer to the shared object.
  constexpr intrusive_ptr(adopt_ref_t, pointer raw_ptr) noexcept
    : ptr_(raw_ptr) {
    // nop
  }

  /// Constructs a new intrusive pointer for managing the lifetime of the object
  /// pointed to by @c raw_ptr.
  ///
  /// This overload adds a new reference.
  ///
  /// @param raw_ptr Pointer to the shared object.
  constexpr intrusive_ptr(new_ref_t, pointer raw_ptr) noexcept : ptr_(raw_ptr) {
    if (ptr_)
      ptr_->ref();
  }

  intrusive_ptr(intrusive_ptr&& other) noexcept : ptr_(other.release()) {
    // nop
  }

  intrusive_ptr(const intrusive_ptr& other) noexcept
    : intrusive_ptr(new_ref, other.get()) {
    // nop
  }

  template <class U, class = std::enable_if_t<std::is_convertible_v<U*, T*>>>
  intrusive_ptr(intrusive_ptr<U> other) noexcept : ptr_(other.release()) {
    // nop
  }

  ~intrusive_ptr() {
    if (ptr_)
      ptr_->unref();
  }

  void swap(intrusive_ptr& other) noexcept {
    std::swap(ptr_, other.ptr_);
  }

  friend void swap(intrusive_ptr& a, intrusive_ptr& b) noexcept {
    using std::swap;
    swap(a.ptr_, b.ptr_);
  }

  /// Detaches an object from the automated lifetime management and sets this
  /// intrusive pointer to @c nullptr.
  /// @returns the raw pointer without modifying the reference count.
  pointer release() noexcept {
    return std::exchange(ptr_, nullptr);
  }

  intrusive_ptr& operator=(const intrusive_ptr& other) noexcept {
    intrusive_ptr tmp{other};
    swap(tmp);
    return *this;
  }

  intrusive_ptr& operator=(intrusive_ptr&& other) noexcept {
    swap(other);
    return *this;
  }

  intrusive_ptr& operator=(std::nullptr_t) noexcept {
    if (ptr_) {
      ptr_->ref();
      ptr_ = nullptr;
    }
    return *this;
  }

  pointer get() const noexcept {
    return ptr_;
  }

  pointer operator->() const noexcept {
    return ptr_;
  }

  reference operator*() const noexcept {
    return *ptr_;
  }

  bool operator!() const noexcept {
    return !ptr_;
  }

  explicit operator bool() const noexcept {
    return ptr_ != nullptr;
  }

  // -- factory functions ------------------------------------------------------

  /// Convenience function for creating a reference counted object and wrapping
  /// it into an intrusive pointers.
  /// @param args Arguments for constructing the shared object of type @c T.
  /// @returns an @c intrusive_ptr pointing to the new object.
  /// @note Assumes that any @c T starts with a reference count of 1.
  template <class... Ts>
  static intrusive_ptr make(Ts&&... args) {
    // Assumes that objects start with a reference count of 1!
    using val_t = std::remove_const_t<T>;
    return {adopt_ref, new val_t(std::forward<Ts>(args)...)};
  }

private:
  pointer ptr_ = nullptr;
};

// -- deduction guides ---------------------------------------------------------

template <class T>
intrusive_ptr(new_ref_t, T*) -> intrusive_ptr<T>;

template <class T>
intrusive_ptr(adopt_ref_t, T*) -> intrusive_ptr<T>;

// -- comparison to nullptr ----------------------------------------------------

/// @relates intrusive_ptr
template <class T>
bool operator==(const intrusive_ptr<T>& x, std::nullptr_t) {
  return !x;
}

/// @relates intrusive_ptr
template <class T>
bool operator==(std::nullptr_t, const intrusive_ptr<T>& x) {
  return !x;
}

/// @relates intrusive_ptr
template <class T>
bool operator!=(const intrusive_ptr<T>& x, std::nullptr_t) {
  return static_cast<bool>(x);
}

/// @relates intrusive_ptr
template <class T>
bool operator!=(std::nullptr_t, const intrusive_ptr<T>& x) {
  return static_cast<bool>(x);
}

// -- comparison to raw pointer ------------------------------------------------

/// @relates intrusive_ptr
template <class T>
bool operator==(const intrusive_ptr<T>& x, const T* y) {
  return x.get() == y;
}

/// @relates intrusive_ptr
template <class T>
bool operator==(const T* x, const intrusive_ptr<T>& y) {
  return x == y.get();
}

/// @relates intrusive_ptr
template <class T>
bool operator!=(const intrusive_ptr<T>& x, const T* y) {
  return x.get() != y;
}

/// @relates intrusive_ptr
template <class T>
bool operator!=(const T* x, const intrusive_ptr<T>& y) {
  return x != y.get();
}

// -- comparison to intrusive pointer ------------------------------------------

// Using trailing return type and decltype() here removes this function from
// overload resolution if the two pointers types are not comparable (SFINAE).

/// @relates intrusive_ptr
template <class T, class U>
auto operator==(const intrusive_ptr<T>& x, const intrusive_ptr<U>& y)
  -> decltype(x.get() == y.get()) {
  return x.get() == y.get();
}

/// @relates intrusive_ptr
template <class T, class U>
auto operator!=(const intrusive_ptr<T>& x, const intrusive_ptr<U>& y)
  -> decltype(x.get() != y.get()) {
  return x.get() != y.get();
}

} // namespace broker

// -- hashing ------------------------------------------------

namespace std {

template <class T>
struct hash<broker::intrusive_ptr<T>> {
  // Hash of intrusive pointer is the same as hash of the raw pointer it holds.
  size_t operator()(const broker::intrusive_ptr<T>& v) const noexcept {
    std::hash<T*> f;
    return f(v.get());
  }
};

} // namespace std
