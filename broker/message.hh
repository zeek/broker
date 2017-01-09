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
  /// Retrieves the contained topic.
  /// @returns A pointer the contained topic or `nullptr` if not present.
  const broker::topic* topic() const;

  /// Retrieves the contained data.
  /// @returns A pointer the contained data or `nullptr` if not present.
  const broker::data* data() const;

  /// Retrieves the contained status.
  /// @returns A pointer the contained status or `nullptr` if not present.
  const broker::status* status() const;

private:
  explicit message(caf::message msg);

  caf::message msg_;
};

} // namespace broker

#endif // BROKER_MESSAGE_HH
