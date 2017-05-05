#include <caf/make_message.hpp>

#include <iostream>


#include "broker/data.hh"
#include "broker/message.hh"
#include "broker/topic.hh"

#include "broker/detail/assert.hh"

namespace broker {

message::message()
  : msg_{caf::make_message(
      broker::topic{},
      caf::make_message(broker::data{}, atom::default_::value))} {
  // nop
}

message::message(broker::data d)
  : msg_{caf::make_message(
      broker::topic{},
      caf::make_message(std::move(d), atom::default_::value))} {
  // nop
}

message::message(broker::topic t, broker::data d)
  : msg_{caf::make_message(
      std::move(t),
      caf::make_message(std::move(d), atom::default_::value))} {
  // nop
}

message::message(broker::topic t, broker::data d, message_type ty)
  : msg_{caf::make_message(
      std::move(t),
      caf::make_message(std::move(d), std::move(ty)))} {
  // nop
}

message::message(broker::topic t, const message& msg)
  : msg_{caf::make_message(std::move(t)) + msg.msg_.drop(1)} {
  // nop
}

const topic& message::topic() const {
  return msg_.get_as<broker::topic>(0);
}

const data& message::data() const {
  return msg_.get_as<caf::message>(1).get_as<broker::data>(0);
}

message_type message::type() const
{
  auto& msg = msg_.get_as<caf::message>(1);

  if ( msg.size() > 1 )
	return msg.get_as<message_type>(1);
  else
        return atom::default_::value;
}

message::message(caf::message msg) : msg_{std::move(msg)} {
  // We only care about the first two elements in the message, but there could
  // be more elements.
  BROKER_ASSERT(msg_.match_element<broker::topic>(0));
  BROKER_ASSERT(msg_.match_element<caf::message>(1));
}

} // namespace broker
