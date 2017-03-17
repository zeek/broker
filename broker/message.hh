#ifndef BROKER_MESSAGE_HH
#define BROKER_MESSAGE_HH

#include <caf/message.hpp>

namespace broker {

class blocking_endpoint;
class data;
class topic;

/// A reference-counted topic-data pair.
class message {
  friend blocking_endpoint; // construction
  friend endpoint; // publish

public:
  /// Default-constructs an empty message.
  message();

  /// Constructs a message from data with an empty topic.
  /// @param d The messsage data.
  explicit message(data d);

  /// Constructs a message as a topic-data pair.
  /// @param t The topic of the message.
  /// @param d The messsage data.
  message(topic t, data d);

  /// Constructs a message from another message with new topic.
  /// @param t The new message topic.
  /// @param msg The message to extract data from.
  message(broker::topic t, const message& msg);

  /// @returns the contained topic.
  const broker::topic& topic() const;

  /// @returns the contained data.
  const broker::data& data() const;

private:
  explicit message(caf::message msg);

  caf::message msg_;
};

} // namespace broker

#endif // BROKER_MESSAGE_HH
