#ifndef BROKER_BLOCKING_ENDPOINT_HH
#define BROKER_BLOCKING_ENDPOINT_HH

#include "broker/endpoint.hh"
#include "broker/mailbox.hh"
#include "broker/message.hh"
#include "broker/status.hh"

namespace broker {

/// An endpoint with a synchronous (blocking) messaging API.
class blocking_endpoint : public endpoint {
  friend context; // construction

public:
  /// Subscribes to a topic.
  /// @param t The topic to subscribe to.
  void subscribe(topic t);

  /// Unsubscribes from a topic.
  /// @param t The topic to unsubscribe from.
  void unsubscribe(topic t);

  /// Consumes the next message blocks until one arrives.
  /// @returns The next message in the mailbox.
  message receive();

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
