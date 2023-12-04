#pragma once

#include <cstddef>
#include <new>
#include <utility>

namespace broker::detail {

// Drop-in replacement for std::pmr::monotonic_buffer_resource.
// TODO: drop this class once the PMR API is available on supported platforms.
class monotonic_buffer_resource {
public:
  monotonic_buffer_resource() {
    allocate_block(nullptr, 0);
  }

  monotonic_buffer_resource(const monotonic_buffer_resource&) = delete;

  monotonic_buffer_resource&
  operator=(const monotonic_buffer_resource&) = delete;

  ~monotonic_buffer_resource() noexcept {
    destroy();
  }

  // Allocates memory.
  [[nodiscard]] void* allocate(size_t bytes,
                               size_t alignment = alignof(max_align_t));

  // Fancy no-op.
  void deallocate(void*, size_t, size_t = alignof(std::max_align_t)) {
    // nop
  }

  template <class T>
  class allocator {
  public:
    using value_type = T;

    template <class U>
    struct rebind {
      using other = allocator<U>;
    };

    constexpr explicit allocator(monotonic_buffer_resource* mbr) : mbr_(mbr) {
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
    monotonic_buffer_resource* mbr_;
  };

private:
  struct block {
    block* next;
    void* bytes;
  };

  void allocate_block(block* prev_block, size_t min_size);

  void destroy() noexcept;

  size_t remaining_ = 0;
  block* current_;
};

// Non-standard convenience function to avoid having to implement a drop-in
// replacement for polymorphic_allocator.
template <class T, class... Args>
T* new_instance(monotonic_buffer_resource& buf, Args&&... args) {
  auto ptr = buf.allocate(sizeof(T), alignof(T));
  return new (ptr) T(std::forward<Args>(args)...);
}

} // namespace broker::detail
