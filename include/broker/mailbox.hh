#pragma once

#include "broker/detail/native_socket.hh"
#include "broker/fwd.hh"

#include <cstddef>

namespace broker::internal {

class flare_actor;
mailbox make_mailbox(flare_actor* actor);

} // namespace broker::internal

namespace broker {

/// A proxy object that represents the mailbox of a blocking endpoint.
class mailbox {
public:
  friend mailbox internal::make_mailbox(internal::flare_actor*);

  /// Retrieves a descriptor that indicates whether a message can be received
  /// without blocking.
  detail::native_socket descriptor();

  /// Checks whether the mailbox is empty.
  bool empty();

  /// Returns the number of messages in the mailbox.
  size_t size();

  /// Returns `size()` (backward compatibility).
  size_t count(size_t = 0);

private:
  explicit mailbox(internal::flare_actor* actor);

  internal::flare_actor* actor_;
};

} // namespace broker
