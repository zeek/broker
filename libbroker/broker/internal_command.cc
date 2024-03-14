#include "broker/internal_command.hh"

namespace broker {

#define TO_STRING_CASE(name)                                                   \
  name:                                                                        \
  return #name;

std::string to_string(command_tag x) {
  switch (x) {
    TO_STRING_CASE(action)
    TO_STRING_CASE(producer_control)
    TO_STRING_CASE(consumer_control)
    default:
      return "???";
  }
}
} // namespace broker
