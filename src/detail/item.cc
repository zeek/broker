#include "broker/detail/item.hh"

namespace broker::detail {

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
