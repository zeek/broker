#include "broker/internal/wire_format.hh"

#include "broker/internal/logger.hh"
#include "broker/message.hh"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/byte_buffer.hpp>
#include <caf/byte_span.hpp>

namespace broker::internal {

bool wire_format::convert(const node_message& msg, caf::byte_buffer& buf) {
  caf::binary_serializer sink{nullptr, buf};
  auto write_bytes = [&sink](caf::const_byte_span bytes) {
    sink.buf().insert(sink.buf().end(), bytes.begin(), bytes.end());
    return true;
  };
  auto write_topic = [&](const auto& x) {
    const auto& str = x.string();
    if (str.size() > 0xFFFF) {
      BROKER_ERROR("topic exceeds maximum size of 65535 characters");
      return false;
    }
    return sink.apply(static_cast<uint16_t>(str.size()))
           && write_bytes(caf::as_bytes(caf::make_span(str)));
  };
  const auto& [sender, receiver, content] = msg.data();
  const auto& [msg_type, ttl, msg_topic, payload] = content.data();
  return sink.apply(sender)                                      //
         && sink.apply(receiver)                                 //
         && sink.apply(msg_type)                                 //
         && sink.apply(ttl)                                      //
         && write_topic(msg_topic)                               //
         && write_bytes(caf::as_bytes(caf::make_span(payload))); //
}

bool wire_format::convert(caf::const_byte_span bytes, node_message& msg) {
  caf::binary_deserializer source{nullptr, bytes};
  auto& [sender, receiver, content] = msg.unshared();
  auto& [msg_type, ttl, msg_topic, payload] = content.unshared();
  // Extract sender, receiver, type and TTL.
  if (!source.apply(sender))
    return false;
  if (!source.apply(receiver))
    return false;
  if (!source.apply(msg_type))
    return false;
  if (!source.apply(ttl))
    return false;
  // Extract topic.
  uint16_t topic_len = 0;
  if (!source.apply(topic_len))
    return false;
  if (auto remainder = source.remainder();
      topic_len == 0 || remainder.size() <= topic_len) {
    return false;
  } else {
    auto str = std::string{reinterpret_cast<const char*>(remainder.data()),
                           topic_len};
    msg_topic = topic{std::move(str)};
    source.skip(topic_len);
  }
  // Extract payload, which simply is the remaining bytes of the message.
  auto remainder = source.remainder();
  auto first = reinterpret_cast<const std::byte*>(remainder.data());
  auto last = first + remainder.size();
  payload.assign(first, last);
  return true;
}

} // namespace broker::internal
