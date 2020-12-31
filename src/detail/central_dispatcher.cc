#include "broker/detail/central_dispatcher.hh"

#include "broker/detail/item_stash.hh"
#include "broker/logger.hh"

namespace broker::detail {

central_dispatcher::central_dispatcher(caf::scheduled_actor* self)
  : alloc_(item_allocator::make()), self_(self) {
  // nop
}

void central_dispatcher::enqueue(caf::span<const item_ptr> ptrs) {
  BROKER_DEBUG("central enqueue" << BROKER_ARG2("ptrs.size", ptrs.size())
                                 << BROKER_ARG2("nested.size", nested_.size()));
  auto i = nested_.begin();
  while (i != nested_.end()) {
    if ((*i)->enqueue(ptrs) >= 0)
      ++i;
    else
      i = nested_.erase(i);
  }
}

void central_dispatcher::ship() {
  for (auto& ptr : nested_)
    ptr->push();
}

void central_dispatcher::add(unipath_manager_ptr out) {
  nested_.emplace_back(std::move(out));
}

item_stash_ptr central_dispatcher::new_item_stash(size_t size) {
  return item_stash::make(alloc_, size);
}

} // namespace broker::detail
