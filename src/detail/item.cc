#include "broker/detail/item.hh"

namespace broker::detail {

item::item(data_message&& msg, uint16_t msg_ttl, caf::stream_manager* origin,
           item_stash* owner)
  : msg_(std::move(msg)), msg_ttl_(msg_ttl), origin_(origin), owner_(owner) {
  // nop
}

item::item(command_message&& msg, uint16_t msg_ttl, caf::stream_manager* origin,
           item_stash* owner)
  : msg_(std::move(msg)), msg_ttl_(msg_ttl), origin_(origin), owner_(owner) {
  // nop
}

void intrusive_ptr_release(item* ptr) noexcept {
  if (--ptr->ref_count_ == 0) {
    auto owner = std::move(ptr->owner_);
    ptr->~item();
    owner->reclaim(ptr);
  }
}

void intrusive_ptr_add_ref(item* ptr) noexcept {
  ++ptr->ref_count_;
}

} // namespace broker::detail
