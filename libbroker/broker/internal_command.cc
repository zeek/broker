#include "broker/internal_command.hh"

namespace broker {

std::string to_string(command_tag x) {
  switch (x) {
    case command_tag::action:
      return "action";
    case command_tag::producer_control:
      return "producer_control";
    case command_tag::consumer_control:
      return "consumer_control";
    default:
      return "???";
  }
}

} // namespace broker
