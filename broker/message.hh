#ifndef BROKER_MESSAGE_HH
#define BROKER_MESSAGE_HH

#include <broker/atoms.hh>

#include <caf/message.hpp>

namespace broker {

class blocking_endpoint;
class data;
class topic;

using message_type = caf::atom_value;
using message_type_default = atom::default_;

template <size_t Size>
constexpr caf::atom_value make_message_type(char const (&str)[Size]) {
  return caf::atom(str);
}

template<caf::atom_value T>
using message_type_constant = caf::atom_constant<T>;

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

  /// Constructs a message with a custom type as a topic-data pair.
  /// @param t The topic of the message.
  /// @param d The messsage data.
  /// @param ty A custom message type.
  message(topic t, data d, message_type ty);

  /// Constructs a message from another message with new topic.
  /// @param t The new message topic.
  /// @param msg The message to extract data from.
  message(broker::topic t, const message& msg);

  /// @returns the contained topic.
  const broker::topic& topic() const;

  /// @returns the contained data.
  const broker::data& data() const;

  /// @return the message type
  message_type type() const;

private:
  explicit message(caf::message msg);

  caf::message msg_;
};

} // namespace broker

#endif // BROKER_MESSAGE_HH
