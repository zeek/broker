#include "broker/detail/monotonic_buffer_resource.hh"

#include <cstdlib>
#include <memory>

namespace broker::detail {

namespace {

// Unlike the standard version, our implementation does *not* follow a geometric
// progression. Simply because our use cases (alm::multipath) allow for a
// simpler implementation.
constexpr size_t block_size = 1024;

} // namespace

monotonic_buffer_resource::monotonic_buffer_resource() {
  current_ = first_block();
  current_->next = nullptr;
  current_->bytes = page0_ + sizeof(block);
  remaining_ = page0_size - sizeof(block);
}

void* monotonic_buffer_resource::allocate(size_t num_bytes, size_t alignment) {
  if (auto res = std::align(alignment, num_bytes,
                            current_->bytes, remaining_)) {
    current_->bytes = static_cast<std::byte*>(res) + num_bytes;
    remaining_ -= num_bytes;
    return res;
  } else {
    allocate_block();
    return allocate(num_bytes, alignment);
  }
}

void monotonic_buffer_resource::allocate_block() {
  if (auto vptr = malloc(block_size)) {
    auto new_block = static_cast<block*>(vptr);
    current_->next = new_block;
    current_ = new_block;
    current_->next = nullptr;
    current_->bytes = static_cast<std::byte*>(vptr) + sizeof(block);
    remaining_ = block_size - sizeof(block);
  } else {
    throw std::bad_alloc();
  }
}

monotonic_buffer_resource::block*
monotonic_buffer_resource::first_block() noexcept {
  return reinterpret_cast<block*>(page0_);
}

void monotonic_buffer_resource::destroy() noexcept {
  auto blk = first_block()->next;
  while (blk != nullptr) {
    auto prev = blk;
    blk = blk->next;
    free(prev);
  }
}

} // namespace broker::detail
