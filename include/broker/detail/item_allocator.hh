#pragma once

#include <cstddef>
#include <vector>

#include <caf/intrusive_ptr.hpp>
#include <caf/span.hpp>

#include "broker/fwd.hh"

namespace broker::detail {

/// Allocates items for internal bookkeeping in the core actor. Never releases
/// its memory until destroyed, but memory usage is bound by the maximum number
/// of peers plus local publishers.
/// @warning the allocator releases all of its memory when destroyed. Hence,
///          clients must make sure return all
/// @warning the embedded reference count is *not* thread-safe!
class item_allocator {
public:
  // -- constants --------------------------------------------------------------

  static constexpr size_t memory_chunk_size = 4'096;

  // -- intrusive_ptr support --------------------------------------------------

  friend void intrusive_ptr_release(item_allocator* ptr) noexcept {
    if (--ptr->ref_count_ == 0)
      delete ptr;
  }

  friend void intrusive_ptr_add_ref(item_allocator* ptr) noexcept {
    ++ptr->ref_count_;
  }

  // -- constructors, destructors, and assignment operators --------------------

  item_allocator(const item_allocator&) = delete;
  item_allocator& operator=(const item_allocator&) = delete;

  ~item_allocator() noexcept;

  static caf::intrusive_ptr<item_allocator> make();

  // -- interface --------------------------------------------------------------

  /// Returns an *uninitialized* item.
  [[nodiscard]] item* allocate();

  /// Fills given range with *uninitialized* items.
  void allocate(caf::span<item*> ptrs);

  /// Puts an *uninitialized* item back to the internal free list.
  void deallocate(item* ptr) noexcept;

  /// Puts all *uninitialized* items back to the internal free list.
  void deallocate(caf::span<item*> ptrs) noexcept;

  /// Returns the total number of bytes held by this allocator.
  size_t allocated_bytes() const noexcept;

  /// Returns the total number of items allocated so far.
  size_t allocated_items() const noexcept;

  /// Returns the number of items that are currently in the free list.
  size_t available_items() const noexcept {
    return free_list_.size();
  }

private:
  item_allocator() = default;

  [[nodiscard]] item* force_allocate();

  size_t ref_count_ = 1;
  std::vector<item*> free_list_;
  std::vector<void*> memory_chunks_;
};

using item_allocator_ptr = caf::intrusive_ptr<item_allocator>;

} // namespace broker::detail
