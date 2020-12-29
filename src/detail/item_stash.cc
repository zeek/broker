#include "broker/detail/item_stash.hh"

#include <new>

#include "broker/detail/assert.hh"
#include "broker/detail/item.hh"

namespace broker::detail {

caf::intrusive_ptr<item_stash> item_stash::make(item_allocator_ptr allocator,
                                                size_t size) {
  using std::swap;
  std::vector<item*> items;
  items.resize(size);
  allocator->allocate(items);
  item_stash_ptr result{new item_stash, false};
  swap(result->stash_, items);
  swap(result->allocator_, allocator);
  return result;
}

item_ptr item_stash::next_item(data_message&& msg, uint16_t msg_ttl,
                               caf::stream_manager* origin) {
  auto ptr = next();
  return item_ptr{new (ptr) item(std::move(msg), msg_ttl, origin, this), false};
}

item_ptr item_stash::next_item(command_message&& msg, uint16_t msg_ttl,
                               caf::stream_manager* origin) {
  auto ptr = next();
  return item_ptr{new (ptr) item(std::move(msg), msg_ttl, origin, this), false};
}

item* item_stash::next() {
  if (!stash_.empty()) {
    auto result = stash_.back();
    stash_.pop_back();
    return result;
  } else {
    throw std::out_of_range("item_stash::next called with available() == 0");
  }
}

void item_stash::reclaim(item* ptr) noexcept {
  BROKER_ASSERT(stash_.capacity() > stash_.size());
  stash_.push_back(ptr);
}

void intrusive_ptr_release(item_stash* ptr) noexcept {
  if (--ptr->ref_count_ == 0) {
    auto items = std::move(ptr->stash_);
    auto alloc = std::move(ptr->allocator_);
    delete ptr;
    alloc->deallocate(items);
  }
}

} // namespace broker::detail
