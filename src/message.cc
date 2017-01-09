#include <caf/actor.hpp>

#include "broker/data.hh"
#include "broker/message.hh"
#include "broker/status.hh"
#include "broker/topic.hh"

#include "broker/detail/assert.hh"

namespace broker {

const topic* message::topic() const {
  if (msg_.match_element<broker::topic>(0))
    return &msg_.get_as<broker::topic>(0);
  return nullptr;
}

const data* message::data() const {
  if (msg_.match_element<caf::message>(1))
    return &msg_.get_as<caf::message>(1).get_as<broker::data>(0);
  return nullptr;
}

const status* message::status() const {
  if (msg_.match_element<broker::status>(0))
    return &msg_.get_as<broker::status>(0);
  return nullptr;
}

message::message(caf::message msg) : msg_{std::move(msg)} {
  BROKER_ASSERT((msg_.match_elements<broker::topic, caf::message, caf::actor>())
                || msg_.match_elements<broker::status>());
}

} // namespace broker
