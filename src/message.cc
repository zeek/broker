#include "broker/data.hh"
#include "broker/message.hh"
#include "broker/topic.hh"

namespace broker {

const topic& message::topic() const {
  return msg_.get_as<broker::topic>(0);
}

const broker::data& message::data() const {
  return msg_.get_as<caf::message>(1).get_as<broker::data>(0);
}

message::message(caf::message msg) : msg_{std::move(msg)} {
  // nop
}

} // namespace broker
