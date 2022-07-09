#include "broker/message.hh"

#include <caf/deep_to_string.hpp>

#include "broker/detail/assert.hh"
#include "broker/internal/type_id.hh"

using namespace std::literals;

namespace broker {

constexpr std::string_view p2p_message_type_names[] = {
  "invalid",        "data",      "command",        "routing_update",
  "ping",           "pong",      "hello",          "probe",
  "version_select", "drop_conn", "originator_syn", "responder_syn_ack",
  "originator_ack",
};

std::string to_string(p2p_message_type x) {
  auto index = static_cast<uint8_t>(x);
  BROKER_ASSERT(index < std::size(p2p_message_type_names));
  return std::string{p2p_message_type_names[index]};
}

bool from_string(std::string_view str, p2p_message_type& x) {
  auto predicate = [&](std::string_view x) { return x == str; };
  auto begin = std::begin(p2p_message_type_names);
  auto end = std::end(p2p_message_type_names);
  auto i = std::find_if(begin, end, predicate);
  if (i == begin || i == end) {
    return false;
  } else {
    x = static_cast<p2p_message_type>(std::distance(begin, i));
    return true;
  }
}

bool from_integer(uint8_t val, p2p_message_type& x) {
  if (val > 0 && val < std::size(p2p_message_type_names)) {
    x = static_cast<p2p_message_type>(val);
    return true;
  } else {
    return false;
  }
}

std::string to_string(packed_message_type x) {
  // Same strings since packed_message is a subset of p2p_message.
  return to_string(static_cast<p2p_message_type>(x));
}

bool from_string(std::string_view str, packed_message_type& x) {
  auto tmp = p2p_message_type{0};
  if (from_string(str, tmp) && static_cast<uint8_t>(tmp) <= 5) {
    x = static_cast<packed_message_type>(tmp);
    return true;
  } else {
    return false;
  }
}

bool from_integer(uint8_t val, packed_message_type& x) {
  if (val <= 0x04) {
    auto tmp = p2p_message_type{0};
    if (from_integer(val, tmp)) {
      x = static_cast<packed_message_type>(tmp);
      return true;
    }
  }
  return false;
}

std::string to_string(const data_message& msg) {
  return caf::deep_to_string(msg.data());
}

std::string to_string(const command_message& msg) {
  return caf::deep_to_string(msg.data());
}

std::string to_string(const node_message& msg) {
  return caf::deep_to_string(msg.data());
}

} // namespace broker
