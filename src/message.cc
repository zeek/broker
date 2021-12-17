#include "broker/message.hh"

#include <caf/deep_to_string.hpp>

#include "broker/internal/type_id.hh"

namespace broker {

std::string to_string(const data_message& msg) {
  return caf::deep_to_string(msg.data());
}


std::string to_string(const command_message& msg) {
  return caf::deep_to_string(msg.data());
}

} // namespace broker
