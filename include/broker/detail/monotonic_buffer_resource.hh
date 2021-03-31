#pragma once

#include <cstddef>
#include <new>

namespace broker::detail {

// Drop-in replacement for std::pmr::monotonic_buffer_resource.
// TODO: drop this class once the PMR API is available on supported platforms.
class monotonic_buffer_resource {
public:
  monotonic_buffer_resource();

  monotonic_buffer_resource(const monotonic_buffer_resource&) = delete;

  monotonic_buffer_resource& operator=(const monotonic_buffer_resource&)
    = delete;

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

private:
  struct block {
    block* next;
    void* bytes;
  };

  void allocate_block();

  block* first_block() noexcept;

  void destroy() noexcept;

  size_t remaining_ = 0;
  block* current_;

  static constexpr size_t page0_size = 1024 - sizeof(size_t) - sizeof(block*);

  std::byte page0_[page0_size ];
};

// Non-standard convenience function to avoid having to implement a drop-in
// replacement for polymorphic_allocator.
template <class T, class... Args>
T* new_instance(monotonic_buffer_resource& buf, Args&&... args) {
  auto ptr = buf.allocate(sizeof(T), alignof(T));
  return new (ptr) T(std::forward<Args>(args)...);
}

} // namespace broker::detail
