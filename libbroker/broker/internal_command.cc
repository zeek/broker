#include "broker/internal_command.hh"

#include "caf/deep_to_string.hpp"

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
