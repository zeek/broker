#pragma once

#include <cstddef>

#include <caf/io/network/native_socket.hpp>

namespace broker {

struct mailbox;

namespace detail {

class flare_actor;
broker::mailbox make_mailbox(flare_actor* actor);

} // namsespace detail

/// A proxy object that represents the mailbox of a blocking endpoint.
struct mailbox {
  friend mailbox detail::make_mailbox(detail::flare_actor*);

public:
  /// Retrieves a descriptor that indicates whether a message can be received
  /// without blocking.
  caf::io::network::native_socket descriptor();

  /// Checks whether the mailbox is empty.
  bool empty();

  /// Returns the number of messages in the mailbox.
  size_t size();

  /// Returns `size()` (backward compatibility).
  size_t count(size_t = 0);

private:
  explicit mailbox(detail::flare_actor* actor);

  detail::flare_actor* actor_;
};

} // namespace broker
