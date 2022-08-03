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

void* monotonic_buffer_resource::allocate(size_t num_bytes, size_t alignment) {
  if (auto res = std::align(alignment, num_bytes, current_->bytes,
                            remaining_)) {
    current_->bytes = static_cast<std::byte*>(res) + num_bytes;
    remaining_ -= num_bytes;
    return res;
  } else {
    allocate_block(current_);
    return allocate(num_bytes, alignment);
  }
}

void monotonic_buffer_resource::allocate_block(block* prev_block) {
  if (auto vptr = malloc(block_size)) {
    current_ = static_cast<block*>(vptr);
    current_->next = prev_block;
    current_->bytes = static_cast<std::byte*>(vptr) + sizeof(block);
    remaining_ = block_size - sizeof(block);
  } else {
    throw std::bad_alloc();
  }
}

void monotonic_buffer_resource::destroy() noexcept {
  auto blk = current_;
  while (blk != nullptr) {
    auto prev = blk;
    blk = blk->next;
    free(prev);
  }
}

} // namespace broker::detail
