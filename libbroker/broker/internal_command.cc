#include "broker/internal_command.hh"

#include "caf/deep_to_string.hpp"

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

void convert(const put_command& x, std::string& str) {
  str = caf::deep_to_string(x);
}

void convert(const put_unique_command& x, std::string& str) {
  str = caf::deep_to_string(x);
}

void convert(const put_unique_result_command& x, std::string& str) {
  str = caf::deep_to_string(x);
}

void convert(const erase_command& x, std::string& str) {
  str = caf::deep_to_string(x);
}

void convert(const expire_command& x, std::string& str) {
  str = caf::deep_to_string(x);
}

void convert(const add_command& x, std::string& str) {
  str = caf::deep_to_string(x);
}

void convert(const subtract_command& x, std::string& str) {
  str = caf::deep_to_string(x);
}

void convert(const clear_command& x, std::string& str) {
  str = caf::deep_to_string(x);
}

void convert(const attach_writer_command& x, std::string& str) {
  str = caf::deep_to_string(x);
}

void convert(const ack_clone_command& x, std::string& str) {
  str = caf::deep_to_string(x);
}

void convert(const cumulative_ack_command& x, std::string& str) {
  str = caf::deep_to_string(x);
}

void convert(const nack_command& x, std::string& str) {
  str = caf::deep_to_string(x);
}

void convert(const keepalive_command& x, std::string& str) {
  str = caf::deep_to_string(x);
}

void convert(const retransmit_failed_command& x, std::string& str) {
  str = caf::deep_to_string(x);
}

void convert(const internal_command& x, std::string& str) {
  str = caf::deep_to_string(x);
}

} // namespace broker
