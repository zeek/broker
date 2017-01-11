#ifndef BROKER_MESSAGE_HH
#define BROKER_MESSAGE_HH

#include <caf/message.hpp>

namespace broker {

class blocking_endpoint;
class data;
class topic;

/// A container for data and status messages.
class message {
  friend blocking_endpoint; // construction

public:
  /// Checks whether a message is a (topic, data) pair.
  /// @returns `true` iff the message contains a (topic, data) pair.
  explicit operator bool() const;

  /// @returns the contained topic.
  /// @pre `static_cast<bool>(*this)`
  const broker::topic& topic() const;

  /// @returns the contained topic.
  /// @pre `static_cast<bool>(*this)`
  const broker::data& data() const;

  /// @returns the contained status.
  /// @pre `!*this`
  const broker::status& status() const;

  /// @returns `true` if the contained status reflects an error.
  /// @pre `!*this`
  bool error() const;

private:
  explicit message(caf::message msg);

  caf::message msg_;
};

} // namespace broker

#endif // BROKER_MESSAGE_HH
