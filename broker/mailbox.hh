#ifndef BROKER_MAILBOX_HH
#define BROKER_MAILBOX_HH

#include <cstddef>
#include <limits>

namespace broker {

namespace detail {
class flare_actor;
} // namsespace detail

class blocking_endpoint;

/// A proxy object that represents the mailbox of a blocking endpoint.
struct mailbox {
  friend blocking_endpoint; // construction

public:
  /// Retrieves a descriptor that indicates whether a message can be received
  /// without blocking.
  int descriptor();

  /// Checks whether the mailbox is empty.
  bool empty();

  /// Counts the number of messages in the mailbox, up to given maximum
  /// @warn This is not a constant-time operations, hence the name `count`
  ///       as opposed to `size`. The function takes time *O(n)* where *n*
  ///       is the size of the mailbox.
  size_t count(size_t max = std::numeric_limits<size_t>::max());

private:
  explicit mailbox(detail::flare_actor* actor);

  detail::flare_actor* actor_;
};

} // namespace broker

#endif // BROKER_MAILBOX_HH
