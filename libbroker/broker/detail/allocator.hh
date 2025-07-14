#pragma once

#include <memory_resource>

namespace broker::detail {

template <class T>
class allocator {
public:
  using value_type = T;

  template <class U>
  struct rebind {
    using other = allocator<U>;
  };

  constexpr explicit allocator(std::pmr::monotonic_buffer_resource* mbr)
    : mbr_(mbr) {
    // nop
  }

  constexpr allocator() : mbr_(nullptr) {
    // nop
  }

  constexpr allocator(const allocator&) = default;

  constexpr allocator& operator=(const allocator&) = default;

  template <class U>
  constexpr allocator(const allocator<U>& other) : mbr_(other.resource()) {
    // nop
  }

  template <class U>
  allocator& operator=(const allocator<U>& other) {
    mbr_ = other.resource();
    return *this;
  }

  T* allocate(size_t n) {
    return static_cast<T*>(mbr_->allocate(sizeof(T) * n, alignof(T)));
  }

  constexpr void deallocate(void*, size_t) noexcept {
    // nop
  }

  constexpr auto resource() const noexcept {
    return mbr_;
  }

  friend bool operator==(allocator& lhs, allocator& rhs) {
    return lhs.mbr_ == rhs.mbr_;
  }

private:
  std::pmr::monotonic_buffer_resource* mbr_;
};

} // namespace broker::detail
