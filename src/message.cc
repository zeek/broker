#include "broker/message.hh"

#include <caf/deep_to_string.hpp>

#include "broker/internal/type_id.hh"

using namespace std::literals;

namespace broker {

std::string to_string(p2p_message_type x) {
  switch (x) {
    case p2p_message_type::data:
      return "data"s;
    case p2p_message_type::command:
      return "command"s;
    case p2p_message_type::routing_update:
      return "routing_update"s;
    case p2p_message_type::hello:
      return "hello"s;
    case p2p_message_type::originator_syn:
      return "originator_syn"s;
    case p2p_message_type::responder_syn_ack:
      return "responder_syn_ack"s;
    case p2p_message_type::originator_ack:
      return "originator_ack"s;
    case p2p_message_type::drop_conn:
      return "drop_conn"s;
    default:
      return "invalid"s;
  }
}

bool from_string(std::string_view str, p2p_message_type& x) {
  if (str == "data"sv) {
    x = p2p_message_type::data;
    return true;
  } else if (str == "command"sv) {
    x = p2p_message_type::command;
    return true;
  } else if (str == "routing_update"sv) {
    x = p2p_message_type::routing_update;
    return true;
  } else if (str == "hello"sv) {
    x = p2p_message_type::hello;
    return true;
  } else if (str == "originator_syn"sv) {
    x = p2p_message_type::originator_syn;
    return true;
  } else if (str == "responder_syn_ack"sv) {
    x = p2p_message_type::responder_syn_ack;
    return true;
  } else if (str == "originator_ack"sv) {
    x = p2p_message_type::originator_ack;
    return true;
  } else if (str == "drop_conn"sv) {
    x = p2p_message_type::drop_conn;
    return true;
  } else {
    return false;
  }
}

bool from_integer(uint8_t val, p2p_message_type& x) {
  switch (val) {
    case 0x01:
      x = p2p_message_type::data;
      return true;
    case 0x02:
      x = p2p_message_type::command;
      return true;
    case 0x03:
      x = p2p_message_type::routing_update;
      return true;
    case 0x10:
      x = p2p_message_type::hello;
      return true;
    case 0x20:
      x = p2p_message_type::originator_syn;
      return true;
    case 0x30:
      x = p2p_message_type::responder_syn_ack;
      return true;
    case 0x40:
      x = p2p_message_type::originator_ack;
      return true;
    case 0x50:
      x = p2p_message_type::drop_conn;
      return true;
    default:
      return false;
  }
}

std::string to_string(packed_message_type x) {
  // Same strings since packed_message is a subset of p2p_message.
  return to_string(static_cast<p2p_message_type>(x));
}

bool from_string(std::string_view str, packed_message_type& x) {
  auto tmp = p2p_message_type{0};
  if (from_string(str, tmp) && static_cast<uint8_t>(tmp) <= 0x04) {
    x = static_cast<packed_message_type>(tmp);
    return true;
  }
  return false;
}

bool from_integer(uint8_t val, packed_message_type& x){
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
