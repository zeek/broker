#include "broker/detail/item_allocator.hh"

#include <cstddef>
#include <cstdlib>
#include <memory>
#include <new>

#include "broker/detail/assert.hh"
#include "broker/detail/item.hh"

namespace broker::detail {

namespace {

constexpr size_t padded_item_size
  = ((sizeof(item) / alignof(item))
     + static_cast<size_t>(sizeof(item) % alignof(item) != 0))
    * alignof(item);

static_assert(padded_item_size >= sizeof(item));

constexpr size_t items_per_memory_chunk
  = item_allocator::memory_chunk_size / padded_item_size;

static_assert(items_per_memory_chunk > 1);

item* item_at_offset(void* chunk, size_t offset) {
  auto ptr = static_cast<void*>(static_cast<char*>(chunk) + offset);
  return static_cast<item*>(ptr);
}

} // namespace

item_allocator::~item_allocator() noexcept {
  BROKER_ASSERT(free_list_.size() == allocated_items());
  for (auto chunk : memory_chunks_)
    free(chunk);
}

caf::intrusive_ptr<item_allocator> item_allocator::make() {
  return {new item_allocator, false};
}

item* item_allocator::allocate() {
  if (!free_list_.empty()) {
    auto result = free_list_.back();
    free_list_.pop_back();
    return result;
  } else {
    return force_allocate();
  }
}

void item_allocator::allocate(caf::span<item*> ptrs) {
  BROKER_ASSERT(!ptrs.empty());
  while (free_list_.size() < ptrs.size()) {
    auto tail = force_allocate();
    free_list_.push_back(tail);
  }
  auto last = free_list_.end();
  auto first = last - ptrs.size();
  std::copy(first, last, ptrs.begin());
  free_list_.erase(first, last);
}

void item_allocator::deallocate(item* ptr) noexcept {
  BROKER_ASSERT(free_list_.capacity() > free_list_.size());
  free_list_.push_back(ptr);
}

void item_allocator::deallocate(caf::span<item*> ptrs) noexcept {
  BROKER_ASSERT(!ptrs.empty());
  BROKER_ASSERT(free_list_.capacity() >= free_list_.size() + ptrs.size());
  free_list_.insert(free_list_.end(), ptrs.begin(), ptrs.end());
}

size_t item_allocator::allocated_bytes() const noexcept {
  return memory_chunks_.size() * memory_chunk_size;
}

size_t item_allocator::allocated_items() const noexcept {
  return memory_chunks_.size() * items_per_memory_chunk;
}

item* item_allocator::force_allocate() {
  // Call potentially throwing functions upfront.
  free_list_.reserve((memory_chunks_.size() + 1) * items_per_memory_chunk);
  memory_chunks_.push_back(nullptr);
  if (auto chunk = malloc(memory_chunk_size)) {
    // Fill free_list_ in reverse order. We dequeue from the back, so clients
    // will actually use memory in the 'correct' order again.
    memory_chunks_.back() = chunk;
    auto offset = items_per_memory_chunk * padded_item_size; // Past the end!
    do {
      offset -= padded_item_size;
      free_list_.push_back(item_at_offset(chunk, offset));
    } while (offset > padded_item_size);
    return item_at_offset(chunk, 0);
  } else {
    memory_chunks_.pop_back();
    throw std::bad_alloc();
  }
}

} // namespace broker::detail
