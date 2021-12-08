#include "broker/message.hh"

#include <caf/deep_to_string.hpp>

namespace broker {

std::string to_string(alm_message_type x) {
  switch (x) {
    case alm_message_type::data:
      return "data";
    case alm_message_type::command:
      return "command";
    case alm_message_type::filter_request:
      return "filter_request";
    case alm_message_type::filter_update:
      return "filter_update";
    case alm_message_type::path_discovery:
      return "path_discovery";
    case alm_message_type::path_revocation:
      return "path_revocation";
    case alm_message_type::hello:
      return "hello";
    case alm_message_type::originator_syn:
      return "originator_syn";
    case alm_message_type::responder_syn_ack:
      return "responder_syn_ack";
    case alm_message_type::originator_ack:
      return "originator_ack";
    case alm_message_type::drop_conn:
      return "drop_conn";
    default:
      return "invalid";
  }
}

bool from_string(caf::string_view str, alm_message_type& x) {
  if (str == "data") {
    x = alm_message_type::data;
    return true;
  } else if (str == "command") {
    x = alm_message_type::command;
    return true;
  } else if (str == "filter_request") {
    x = alm_message_type::filter_request;
    return true;
  } else if (str == "filter_update") {
    x = alm_message_type::filter_update;
    return true;
  } else if (str == "path_discovery") {
    x = alm_message_type::path_discovery;
    return true;
  } else if (str == "path_revocation") {
    x = alm_message_type::path_revocation;
    return true;
  } else if (str == "hello") {
    x = alm_message_type::hello;
    return true;
  } else if (str == "originator_syn") {
    x = alm_message_type::originator_syn;
    return true;
  } else if (str == "responder_syn_ack") {
    x = alm_message_type::responder_syn_ack;
    return true;
  } else if (str == "originator_ack") {
    x = alm_message_type::originator_ack;
    return true;
  } else if (str == "drop_conn") {
    x = alm_message_type::drop_conn;
    return true;
  } else {
    return false;
  }
}

bool from_integer(uint8_t val, alm_message_type& x) {
  switch (val) {
    case 0x01:
      x = alm_message_type::data;
      return true;
    case 0x02:
      x = alm_message_type::command;
      return true;
    case 0x03:
      x = alm_message_type::filter_request;
      return true;
    case 0x04:
      x = alm_message_type::filter_update;
      return true;
    case 0x05:
      x = alm_message_type::path_discovery;
      return true;
    case 0x06:
      x = alm_message_type::path_revocation;
      return true;
    case 0x10:
      x = alm_message_type::hello;
      return true;
    case 0x20:
      x = alm_message_type::originator_syn;
      return true;
    case 0x30:
      x = alm_message_type::responder_syn_ack;
      return true;
    case 0x40:
      x = alm_message_type::originator_ack;
      return true;
    case 0x50:
      x = alm_message_type::drop_conn;
      return true;
    default:
      return false;
  }
}

std::string to_string(packed_message_type x) {
  // Same strings since packed_message is a subset of alm_message.
  return to_string(static_cast<alm_message_type>(x));
}

bool from_string(caf::string_view str, packed_message_type& x) {
  auto tmp = alm_message_type{0};
  if (from_string(str, tmp) && static_cast<uint8_t>(tmp) <= 0x06) {
    x = static_cast<packed_message_type>(tmp);
    return true;
  }
  return false;
}

bool from_integer(uint8_t val, packed_message_type& x){
  if (val <= 0x06) {
    auto tmp = alm_message_type{0};
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

namespace broker::detail{

namespace {

thread_local topic_cache_type topic_cache;

thread_local path_cache_type path_cache;

thread_local content_buf_type content_buf;

} // namespace

topic_cache_type& thread_local_topic_cache() {
  return topic_cache;
}

path_cache_type& thread_local_path_cache() {
  return path_cache;
}

content_buf_type& thread_local_content_buf() {
  return content_buf;
}

} // namespace broker::detail
