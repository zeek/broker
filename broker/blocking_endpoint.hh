#ifndef BROKER_BLOCKING_ENDPOINT_HH
#define BROKER_BLOCKING_ENDPOINT_HH

#include <caf/blocking_actor.hpp>

#include "broker/endpoint.hh"

namespace broker {

namespace detail {
class flare_actor;
} // namsespace detail

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

/// An endpoint with a synchronous (blocking) messaging API.
class blocking_endpoint : public endpoint {
  friend context; // construction

public:
  /// Consumes the next message in the mailbox or blocks until one arrives.
  /// @returns The next message in the mailbox.
  message receive();

  /// Consumes one message that matches the given handler.
  template <class T, class... Ts>
  void receive(T&& x, Ts&&... xs) {
    auto subscriber = caf::actor_cast<caf::blocking_actor*>(subscriber_);
    subscriber->receive(std::forward<T>(x), std::forward<Ts>(xs)...);
  }

  /// Access the endpoint's mailbox, which provides the following
  /// introspection functions:
  ///
  /// - `int descriptor()`: Retrieves a descriptor that indicates whether a
  ///   message can be received without blocking.
  ///
  /// - `bool empty()`: Checks whether the endpoint's message mailbox is empty.
  ///
  /// - `size_t count(size_t max)`: Counts the number of messages in the
  ///   mailbox in time that is a linear function of the mailbox size. The
  ///   parameter `max` allows for specifying an upper bound when to stop
  ///   counting.
  ///
  /// @returns A proxy object to introspect the endpoint's mailbox.
  broker::mailbox mailbox();

private:
  blocking_endpoint(caf::actor_system& sys);
};

} // namespace broker

#endif // BROKER_BLOCKING_ENDPOINT_HH
