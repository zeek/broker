#include "broker/envelope.hh"

#include "broker/command_envelope.hh"
#include "broker/data_envelope.hh"
#include "broker/defaults.hh"
#include "broker/detail/monotonic_buffer_resource.hh"
#include "broker/error.hh"
#include "broker/expected.hh"
#include "broker/format/bin.hh"
#include "broker/internal/logger.hh"
#include "broker/internal/type_id.hh"
#include "broker/p2p_message_type.hh"
#include "broker/ping_envelope.hh"
#include "broker/pong_envelope.hh"
#include "broker/routing_update_envelope.hh"
#include "broker/topic.hh"
#include "broker/variant.hh"
#include "broker/variant_data.hh"

#include <caf/byte_buffer.hpp>
#include <caf/detail/ieee_754.hpp>
#include <caf/detail/network_order.hpp>

namespace broker {

// -- utilities ----------------------------------------------------------------

namespace {

template <class T>
using mbr_allocator = broker::detail::monotonic_buffer_resource::allocator<T>;

using const_byte_pointer = const std::byte*;

} // namespace

// -- envelope_type ------------------------------------------------------------

std::string to_string(envelope_type x) {
  // Same strings since packed_message is a subset of p2p_message.
  return to_string(static_cast<p2p_message_type>(x));
}

bool from_string(std::string_view str, envelope_type& x) {
  auto tmp = p2p_message_type{0};
  if (from_string(str, tmp) && static_cast<uint8_t>(tmp) <= 5) {
    x = static_cast<envelope_type>(tmp);
    return true;
  } else {
    return false;
  }
}

bool from_integer(uint8_t val, envelope_type& x) {
  if (val <= 0x04) {
    auto tmp = p2p_message_type{0};
    if (from_integer(val, tmp)) {
      x = static_cast<envelope_type>(tmp);
      return true;
    }
  }
  return false;
}

// -- envelope -----------------------------------------------------------------

envelope::~envelope() {
  // nop
}

uint16_t envelope::ttl() const noexcept {
  return defaults::ttl;
}

endpoint_id envelope::sender() const noexcept {
  return endpoint_id::nil();
}

endpoint_id envelope::receiver() const noexcept {
  return endpoint_id::nil();
}

expected<envelope_ptr> envelope::deserialize(const std::byte* data,
                                             size_t size) {
  // Format is as follows:
  // -  16 bytes: sender
  // -  16 bytes: receiver
  // -   1 byte : message type
  // -   2 bytes: TTL
  // -   2 bytes: topic length T
  // -   T bytes: topic
  // - remainder: payload (type-specific)
  if (size < 37) {
    BROKER_ERROR("envelope::deserialize failed: message too short");
    return make_error(ec::invalid_data, "message too short");
  }
  auto advance = [&](size_t n) {
    data += n;
    size -= n;
  };
  // Extract the sender.
  auto sender = endpoint_id::from_bytes(data);
  advance(16);
  // Extract the receiver.
  auto receiver = endpoint_id::from_bytes(data);
  advance(16);
  // Extract the type.
  auto msg_type = static_cast<envelope_type>(*data);
  advance(1);
  // Extract the TTL.
  auto ttl = uint16_t{0};
  memcpy(&ttl, data, sizeof(ttl));
  ttl = format::bin::v1::from_network_order(ttl);
  advance(2);
  // Extract the topic.
  auto topic_size = uint16_t{0};
  memcpy(&topic_size, data, sizeof(topic_size));
  topic_size = format::bin::v1::from_network_order(topic_size);
  advance(2);
  if (topic_size > size)
    return make_error(ec::invalid_data, "invalid topic size");
  auto topic_str = std::string_view{reinterpret_cast<const char*>(data),
                                    topic_size};
  advance(topic_size);
  // Extract the payload by delegating to the type-specific deserializer.
  switch (msg_type) {
    default:
      BROKER_ERROR("envelope::deserialize failed: invalid message type");
      return make_error(ec::invalid_data, "invalid message type");
    case envelope_type::data:
      if (auto res = data_envelope::deserialize(sender, receiver, ttl,
                                                topic_str, data, size))
        return *res;
      else
        return res.error();
    case envelope_type::command:
      return command_envelope::deserialize(sender, receiver, ttl, topic_str,
                                           data, size);
    case envelope_type::routing_update:
      return routing_update_envelope::deserialize(sender, receiver, ttl,
                                                  topic_str, data, size);
    case envelope_type::ping:
      return ping_envelope::deserialize(sender, receiver, ttl, topic_str, data,
                                        size);
    case envelope_type::pong:
      return pong_envelope::deserialize(sender, receiver, ttl, topic_str, data,
                                        size);
  }
}

data_envelope_ptr envelope::as_data() {
  BROKER_ASSERT(type() == envelope_type::data);
  return {new_ref, static_cast<data_envelope*>(this)};
}

command_envelope_ptr envelope::as_command() {
  BROKER_ASSERT(type() == envelope_type::command);
  return {new_ref, static_cast<command_envelope*>(this)};
}

routing_update_envelope_ptr envelope::as_routing_update() {
  BROKER_ASSERT(type() == envelope_type::routing_update);
  return {new_ref, static_cast<routing_update_envelope*>(this)};
}

ping_envelope_ptr envelope::as_ping() {
  BROKER_ASSERT(type() == envelope_type::ping);
  return {new_ref, static_cast<ping_envelope*>(this)};
}

pong_envelope_ptr envelope::as_pong() {
  BROKER_ASSERT(type() == envelope_type::pong);
  return {new_ref, static_cast<pong_envelope*>(this)};
}

envelope_type data_envelope::type() const noexcept {
  return envelope_type::data;
}

} // namespace broker
