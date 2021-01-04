#pragma once

#include <caf/stream_manager.hpp>

#include "broker/detail/item_scope.hh"
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

  bool is_data_message() const noexcept {
    return caf::holds_alternative<data_message>(msg_);
  }

  bool is_command_message() const noexcept {
    return caf::holds_alternative<command_message>(msg_);
  }

  /// @pre `is_data_message()`
  const auto& as_data_message() const noexcept {
    return caf::get<data_message>(msg_);
  }

  /// @pre `is_command_message()`
  const auto& as_command_message() const noexcept {
    return caf::get<command_message>(msg_);
  }

  const auto& msg() const noexcept {
    return msg_;
  }

  bool unique() const noexcept {
    return ref_count_ == 1;
  }

  uint16_t ttl() const noexcept {
    return msg_ttl_;
  }

  item_scope scope() const noexcept {
    return scope_;
  }

  const caf::stream_manager_ptr& origin() const noexcept {
    return origin_;
  }

private:
  // -- constructors, destructors, and assignment operators --------------------

  template <class T>
  item(T&& msg, uint16_t msg_ttl, caf::stream_manager* origin,
       item_stash* owner, item_scope scope)
    : msg_(std::forward<T>(msg)),
      msg_ttl_(msg_ttl),
      scope_(scope),
      origin_(origin),
      owner_(owner) {
    // nop
  }

  // -- member variables -------------------------------------------------------

  size_t ref_count_ = 1;
  node_message_content msg_;
  uint16_t msg_ttl_;
  item_scope scope_;
  caf::stream_manager_ptr origin_;
  item_stash_ptr owner_;
};

using item_ptr = caf::intrusive_ptr<item>;

} // namespace broker::detail
