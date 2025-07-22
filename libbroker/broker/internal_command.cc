#include "broker/internal_command.hh"
#include "broker/internal/type_id.hh"

#include "caf/deep_to_string.hpp"

namespace broker {

namespace {

// Usually, we could simply call caf::deep_to_string. However, Broker injects a
// `to_string` function for any type offering `convert`. Because of this greedy
// template, calling `caf::deep_to_string` would simply call `convert` again,
// leading to an endless recursion. Calling the `inspect` overload here manually
// avoids this issue.
template <class T>
void do_stringify(const T& what, std::string& out) {
  caf::detail::stringification_inspector f{out};
  broker::inspect(f, const_cast<T&>(what));
}

} // namespace

void convert(const command_tag& x, std::string& str) {
  switch (x) {
    case command_tag::action:
      str = "action";
      break;
    case command_tag::producer_control:
      str = "producer_control";
      break;
    case command_tag::consumer_control:
      str = "consumer_control";
      break;
    default:
      str = "???";
      break;
  }
}

void convert(const put_command& x, std::string& str) {
  do_stringify(x, str);
}

void convert(const put_unique_command& x, std::string& str) {
  do_stringify(x, str);
}

void convert(const put_unique_result_command& x, std::string& str) {
  do_stringify(x, str);
}

void convert(const erase_command& x, std::string& str) {
  do_stringify(x, str);
}

void convert(const expire_command& x, std::string& str) {
  do_stringify(x, str);
}

void convert(const add_command& x, std::string& str) {
  do_stringify(x, str);
}

void convert(const subtract_command& x, std::string& str) {
  do_stringify(x, str);
}

void convert(const clear_command& x, std::string& str) {
  do_stringify(x, str);
}

void convert(const attach_writer_command& x, std::string& str) {
  do_stringify(x, str);
}

void convert(const ack_clone_command& x, std::string& str) {
  do_stringify(x, str);
}

void convert(const cumulative_ack_command& x, std::string& str) {
  do_stringify(x, str);
}

void convert(const nack_command& x, std::string& str) {
  do_stringify(x, str);
}

void convert(const keepalive_command& x, std::string& str) {
  do_stringify(x, str);
}

void convert(const retransmit_failed_command& x, std::string& str) {
  do_stringify(x, str);
}

void convert(const internal_command& x, std::string& str) {
  do_stringify(x, str);
}

} // namespace broker
