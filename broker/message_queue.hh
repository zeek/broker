#ifndef BROKER_MESSAGE_QUEUE_HH
#define BROKER_MESSAGE_QUEUE_HH

#include <memory>

#include "broker/endpoint.hh"
#include "broker/message.hh"
#include "broker/queue.hh"
#include "broker/topic.hh"

namespace broker {

/// Requests messages from a broker::endpoint or its peers that match a topic
/// prefix.
class message_queue : public queue<broker::message> {
public:
  /// Create an uninitialized message queue.
  message_queue();

  /// Attach a message_queue to an endpoint.
  /// @param prefix the subscription topic to use.  All messages sent via
  /// endpoint *e* or one of its peers that use a topic prefixed by *prefix*
  /// will be copied in to this queue.
  /// @param e the endpoint to attach the message_queue.
  message_queue(topic prefix, const endpoint& e);

  /// @return the subscription topic prefix of the queue.
  const topic& get_topic_prefix() const;

private:
  topic subscription_prefix_;
};

} // namespace broker

#endif // BROKER_MESSAGE_QUEUE_HH
