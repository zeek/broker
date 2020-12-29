#pragma once

#include <caf/stream_manager.hpp>

#include "broker/detail/item_stash.hh"
#include "broker/fwd.hh"
#include "broker/message.hh"

namespace broker::detail {

/// @warning the embedded reference count is *not* thread-safe by design! Items
///          as well as the stash and its allocator are local to a single actor.
class item {
public:
  // -- friends ----------------------------------------------------------------

  friend item_stash;

  // -- intrusive_ptr support --------------------------------------------------

  friend void intrusive_ptr_release(item*) noexcept;

  friend void intrusive_ptr_add_ref(item*) noexcept;

  // -- properties -------------------------------------------------------------

  bool is_data_message() const noexcept;

  bool is_command_message() const noexcept;

  uint16_t ttl() const noexcept {
    return msg_ttl_;
  }

  const caf::stream_manager_ptr& origin() const noexcept {
    return origin_;
  }

private:
  // -- constructors, destructors, and assignment operators --------------------

  item(data_message&& msg, uint16_t msg_ttl, caf::stream_manager* origin,
       item_stash* owner);

  item(command_message&& msg, uint16_t msg_ttl, caf::stream_manager* origin,
       item_stash* owner);

  // -- member variables -------------------------------------------------------

  size_t ref_count_ = 1;
  caf::variant<data_message, command_message> msg_;
  uint16_t msg_ttl_;
  caf::stream_manager_ptr origin_;
  item_stash_ptr owner_;
};

using item_ptr = caf::intrusive_ptr<item>;

} // namespace broker::detail
