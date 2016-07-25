#ifndef BROKER_MESSAGE_HH
#define BROKER_MESSAGE_HH

#include <caf/message.hpp>

namespace broker {

class blocking_endpoint;
class data;
class topic;

/// A container for data.
class message {
  friend blocking_endpoint; // construction

public:
  const broker::topic& topic() const;
  const broker::data& data() const;

private:
  explicit message(caf::message msg);

  caf::message msg_; // <topic, <data>, actor>
};

} // namespace broker

#endif // BROKER_MESSAGE_HH
