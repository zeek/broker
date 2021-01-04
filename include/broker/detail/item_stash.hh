#pragma once

#include <cstddef>
#include <vector>

#include "broker/detail/item_allocator.hh"
#include "broker/detail/item_scope.hh"
#include "broker/fwd.hh"

namespace broker::detail {

class item_stash {
public:
  // -- intrusive_ptr support --------------------------------------------------

  friend void intrusive_ptr_release(item*) noexcept;

  friend void intrusive_ptr_release(item_stash*) noexcept;

  friend void intrusive_ptr_add_ref(item_stash* ptr) noexcept {
    ++ptr->ref_count_;
  }

  // -- constructors, destructors, and assignment operators --------------------

  static caf::intrusive_ptr<item_stash> make(item_allocator_ptr allocator,
                                             size_t size);

  // -- size management --------------------------------------------------------

  /// Makes `n` more items available to the stash.
  void replenish(size_t n);

  // -- properties -------------------------------------------------------------

  size_t available() const noexcept {
    return stash_.size();
  }

  bool empty() const noexcept {
    return stash_.empty();
  }

  // -- item factory functions -------------------------------------------------

  item_ptr next_item(data_message&& msg, uint16_t msg_ttl,
                     caf::stream_manager* origin,
                     item_scope scope = item_scope::global);

  item_ptr next_item(command_message&& msg, uint16_t msg_ttl,
                     caf::stream_manager* origin,
                     item_scope scope = item_scope::global);

  item_ptr next_item(node_message_content&& msg, uint16_t msg_ttl,
                     caf::stream_manager* origin,
                     item_scope scope = item_scope::global);

private:
  // -- constructors, destructors, and assignment operators --------------------

  item_stash() = default;

  // -- item management --------------------------------------------------------

  [[nodiscard]] item* next();

  void reclaim(item* ptr) noexcept;

  // -- member variables -------------------------------------------------------

  size_t ref_count_ = 1;
  std::vector<item*> stash_;
  item_allocator_ptr allocator_;
  size_t max_stash_size_ = 0;
};

using item_stash_ptr = caf::intrusive_ptr<item_stash>;

} // namespace broker::detail
