#pragma once

#include <vector>

#include <caf/fwd.hpp>

#include "broker/detail/item_allocator.hh"
#include "broker/detail/item_stash.hh"
#include "broker/detail/unipath_manager.hh"
#include "broker/fwd.hh"

namespace broker::detail {

/// Central point for all `unipath_manager` instances to enqueue items.
class central_dispatcher {
public:
  explicit central_dispatcher(caf::scheduled_actor* self);

  /// Enqueues given items to all nested output paths.
  void enqueue(caf::span<const item_ptr> ptrs);

  /// Tries to emit batches on all nested output paths.
  void ship();

  /// Adds a new output path to the dispatcher.
  void add(unipath_manager_ptr out);

  /// Creates a new item stash to generate items for this dispatcher.
  item_stash_ptr new_item_stash(size_t size);

  auto self() const noexcept {
    return self_;
  }

private:
  std::vector<unipath_manager_ptr> nested_;
  item_allocator_ptr alloc_;
  caf::scheduled_actor* self_;
};

} // namespace broker::detail
