#include "broker/internal/wire_format.hh"

#include "broker/envelope.hh"
#include "broker/expected.hh"
#include "broker/format/bin.hh"
#include "broker/internal/logger.hh"
#include "broker/internal/native.hh"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/byte_buffer.hpp>
#include <caf/byte_span.hpp>

using namespace std::literals;

#define WIRE_FORMAT_TYPE_NAME(type)                                            \
  namespace caf {                                                              \
  template <>                                                                  \
  struct type_name<broker::internal::wire_format::type> {                      \
    static constexpr string_view value = #type;                                \
  };                                                                           \
  }

WIRE_FORMAT_TYPE_NAME(var_msg_error)
WIRE_FORMAT_TYPE_NAME(hello_msg)
WIRE_FORMAT_TYPE_NAME(probe_msg)
WIRE_FORMAT_TYPE_NAME(version_select_msg)
WIRE_FORMAT_TYPE_NAME(drop_conn_msg)
WIRE_FORMAT_TYPE_NAME(v1::originator_syn_msg)
WIRE_FORMAT_TYPE_NAME(v1::responder_syn_ack_msg)
WIRE_FORMAT_TYPE_NAME(v1::originator_ack_msg)

namespace broker::internal::wire_format {

std::pair<ec, std::string_view> check(const hello_msg& x) {
  if (x.magic != magic_number)
    return {ec::wrong_magic_number, "wrong magic number"};
  else if (x.min_version > protocol_version || x.max_version < protocol_version)
    return {ec::peer_incompatible, "unsupported versions offered"};
  else
    return {ec::none, {}};
}

std::pair<ec, std::string_view> check(const probe_msg& x) {
  if (x.magic != magic_number)
    return {ec::wrong_magic_number, "wrong magic number"};
  else
    return {ec::none, {}};
}

std::pair<ec, std::string_view> check(const version_select_msg& x) {
  if (x.magic != magic_number)
    return {ec::wrong_magic_number, "wrong magic number"};
  else if (x.selected_version != protocol_version)
    return {ec::peer_incompatible, "unsupported version selected"};
  else
    return {ec::none, {}};
}

std::pair<ec, std::string_view> check(const drop_conn_msg& x) {
  if (x.magic != magic_number)
    return {ec::wrong_magic_number, "wrong magic number"};
  else if (!convertible_to_ec(x.code))
    return {ec::unspecified, x.description};
  else
    return {ec::none, {}};
}

namespace v1 {

bool trait::convert(const envelope_ptr& msg, caf::byte_buffer& buf) {
  caf::binary_serializer sink{nullptr, buf};
  auto write_topic = [&msg, &sink] {
    auto str = msg->topic();
    if (str.size() > 0xFFFF) {
      BROKER_ERROR("topic exceeds maximum size of 65,535 characters");
      sink.emplace_error(caf::sec::invalid_argument,
                         "topic exceeds maximum size of 65,535 characters");
      return false;
    }
    if (!sink.apply(static_cast<uint16_t>(str.size()))) {
      BROKER_ERROR("failed to write topic size");
      return false;
    }
    auto first = reinterpret_cast<const caf::byte*>(str.data());
    sink.buf().insert(sink.buf().end(), first, first + str.size());
    return true;
  };
  auto write_payload = [&msg, &sink] {
    auto [data, size] = msg->raw_bytes();
    auto first = reinterpret_cast<const caf::byte*>(data);
    sink.buf().insert(sink.buf().end(), first, first + size);
    return true;
  };
  auto ok = sink.apply(msg->sender())      //
            && sink.apply(msg->receiver()) //
            && sink.apply(msg->type())     //
            && sink.apply(msg->ttl())      //
            && write_topic()               //
            && write_payload();            //
  if (!ok)
    last_error_ = sink.get_error();
  return ok;
}

bool trait::convert(caf::const_byte_span bytes, envelope_ptr& msg) {
  auto data = reinterpret_cast<const std::byte*>(bytes.data());
  auto res = envelope::deserialize(data, bytes.size());
  if (!res) {
    last_error_ = std::move(native(res.error()));
    return false;
  }
  msg = std::move(*res);
  return true;
}

} // namespace v1

// Note: calling this to_string blows up, since CAF picks up to_string over
//       inspect and std::variant is implicitly convertible from the message
//       types.
std::string stringify(const var_msg& msg) {
  auto fn = [](const auto& x) { return caf::deep_to_string(x); };
  return std::visit(fn, msg);
}

#define MSG_CASE(type)                                                         \
  case type::tag: {                                                            \
    type tmp;                                                                  \
    if (!src.apply(tmp)) {                                                     \
      return make_var_msg_error(ec::invalid_message,                           \
                                "failed to parse " #type ":"                   \
                                  + to_string(src.get_error()));               \
    } else if (auto [code, descr] = check(tmp); code != ec::none) {            \
      return make_var_msg_error(code, std::string{descr});                     \
    } else {                                                                   \
      return {std::move(tmp)};                                                 \
    }                                                                          \
  }

var_msg decode(caf::const_byte_span bytes) {
  caf::binary_deserializer src{nullptr, bytes};
  auto msg_type = p2p_message_type{0};
  if (!src.apply(msg_type))
    return make_var_msg_error(ec::invalid_message, "invalid message type tag"s);
  switch (msg_type) {
    MSG_CASE(hello_msg)
    MSG_CASE(probe_msg)
    MSG_CASE(version_select_msg)
    MSG_CASE(drop_conn_msg)
    MSG_CASE(v1::originator_syn_msg)
    MSG_CASE(v1::responder_syn_ack_msg)
    MSG_CASE(v1::originator_ack_msg)
    default:
      break;
  }
  return make_var_msg_error(ec::invalid_message, "invalid message type tag"s);
}

} // namespace broker::internal::wire_format
