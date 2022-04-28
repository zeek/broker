#pragma once

#include <cstddef>
#include <utility>

namespace broker::detail {

/// An opaque type with no implementation. Only used for casting between opaque
/// handles and internal types. Internally, this is always a `caf::ref_counted`.
struct opaque_type;

/// @relates opaque_type
void intrusive_ptr_add_ref(const opaque_type*) noexcept;

/// @relates opaque_type
void intrusive_ptr_release(const opaque_type*) noexcept;

/// A reference counting pointer for opaque types.
/// @relates opaque_type
class opaque_ptr {
public:
  // -- member types -----------------------------------------------------------

  using pointer = opaque_type*;

  // -- constructors, destructors, and assignment operators --------------------

  constexpr opaque_ptr() noexcept : ptr_(nullptr) {
    // nop
  }

  constexpr opaque_ptr(std::nullptr_t) noexcept : opaque_ptr() {
    // nop
  }

  explicit opaque_ptr(pointer raw_ptr, bool add_ref) noexcept : opaque_ptr() {
    reset(raw_ptr, add_ref);
  }

  opaque_ptr(opaque_ptr&& other) noexcept : ptr_(other.release()) {
    // nop
  }

  opaque_ptr(const opaque_ptr& other) noexcept {
    reset(other.get());
  }

  opaque_ptr& operator=(opaque_ptr&& other) noexcept {
    swap(other);
    return *this;
  }

  opaque_ptr& operator=(const opaque_ptr& other) noexcept {
    reset(other.ptr_);
    return *this;
  }

  ~opaque_ptr() noexcept {
    if (ptr_)
      intrusive_ptr_release(ptr_);
  }

  void swap(opaque_ptr& other) noexcept {
    std::swap(ptr_, other.ptr_);
  }

  void reset(pointer ptr = nullptr, bool add_ref = true) noexcept {
    if (ptr_)
      intrusive_ptr_release(ptr_);
    ptr_ = ptr;
    if (ptr_ && add_ref)
      intrusive_ptr_add_ref(ptr_);
  }

  pointer release() noexcept {
    auto result = ptr_;
    if (result)
      ptr_ = nullptr;
    return result;
  }

  pointer get() const noexcept {
    return ptr_;
  }

  bool operator!() const noexcept {
    return !ptr_;
  }

  explicit operator bool() const noexcept {
    return ptr_ != nullptr;
  }

private:
  pointer ptr_;
};

} // namespace broker::detail
