#pragma once

#include "broker/endpoint_id.hh"
#include "broker/error.hh"
#include "broker/fwd.hh"
#include "broker/message.hh"

#include <caf/byte_buffer.hpp>
#include <caf/byte_span.hpp>
#include <caf/error.hpp>
#include <caf/fwd.hpp>

// After establishing a transport channel (usually TCP/TLS), the Broker protocol
// traverses three phases:
// - Phase 1 negotiates the protocol version between two peers.
// - Phase 2 exchanges state such as subscriptions for the actual peering.
// - Phase 3 represents the "operational mode" where Broker endpoints exchange
//   data and store messages.

namespace broker::internal::wire_format {

// -- constants ----------------------------------------------------------------

/// Magic number for hello and probe messages (the first messages Broker
/// exchanges). These are the ASCII codes for 'ZEEK' in hexadecimal.
constexpr uint32_t magic_number = 0x5A45454B;

/// The current version of the protocol.
constexpr uint8_t protocol_version = 1;

// -- version-agnostic Broker messages -----------------------------------------

/// Starts the handshake process. Sent by the Broker node that establishes the
/// TCP connection. To avoid ordering issues for concurrent handshakes, the
/// Broker endpoint with the smaller ID becomes the originator. When receiving a
/// `hello` message with a smaller ID, a Broker endpoint responds with a `hello`
/// message on its own.
struct hello_msg {
  static constexpr auto tag = p2p_message_type::hello;

  /// The magic number to make sure we are talking to a Broker endpoint. Must be
  /// @ref magic_number.
  uint32_t magic = 0;

  /// The ID of the sender.
  endpoint_id sender_id;

  /// The minimal protocol version supported by the sender.
  uint8_t min_version = 0;

  /// The maximum (preferred) protocol version supported by the sender.
  uint8_t max_version = 0;
};

/// @relates hello_msg
std::pair<ec, std::string_view> check(const hello_msg& x);

/// @relates hello_msg
template <class Inspector>
bool inspect(Inspector& f, hello_msg& x) {
  return f.object(x).fields(f.field("magic", x.magic),
                            f.field("sender-id", x.sender_id),
                            f.field("min-version", x.min_version),
                            f.field("max-version", x.max_version));
}

/// @relates hello_msg
inline hello_msg make_hello_msg(endpoint_id id) {
  return {magic_number, id, protocol_version, protocol_version};
}

/// Only probes connectivity without any other effect. Sent as first message by
/// after accepting an incoming connection to provoke errors early.
struct probe_msg {
  static constexpr auto tag = p2p_message_type::probe;

  /// The magic number to make sure we are talking to a Broker endpoint. Must be
  /// @ref magic_number.
  uint32_t magic = 0;
};

/// @relates probe_msg
std::pair<ec, std::string_view> check(const probe_msg& x);

/// @relates probe_msg
template <class Inspector>
bool inspect(Inspector& f, probe_msg& x) {
  return f.object(x).fields(f.field("magic", x.magic));
}

/// @relates probe_msg
inline probe_msg make_probe_msg() {
  return {magic_number};
}

/// Switches to version-specific message types (Phase 2).
struct version_select_msg {
  static constexpr auto tag = p2p_message_type::version_select;

  /// The magic number to make sure we are talking to a Broker endpoint. Must be
  /// @ref magic_number.
  uint32_t magic = 0;

  /// The ID of the sender.
  endpoint_id sender_id;

  /// The minimal protocol version supported by the sender.
  uint8_t selected_version = 0;
};

/// @relates version_select_msg
std::pair<ec, std::string_view> check(const version_select_msg& x);

/// @relates version_select_msg
template <class Inspector>
bool inspect(Inspector& f, version_select_msg& x) {
  return f.object(x).fields(f.field("magic", x.magic),
                            f.field("sender-id", x.sender_id),
                            f.field("selected-version", x.selected_version));
}

/// @relates probe_msg
inline version_select_msg make_version_select_msg(endpoint_id id) {
  return {magic_number, id, protocol_version};
}

/// Aborts the handshake.
struct drop_conn_msg {
  static constexpr auto tag = p2p_message_type::drop_conn;

  /// The magic number to make sure we are talking to a Broker endpoint. Must be
  /// @ref magic_number.
  uint32_t magic = 0;

  /// The ID of the sender.
  endpoint_id sender_id;

  /// Error code, i.e., the integer value of an @ref ec.
  uint8_t code = 0;

  /// Some human-readable description for the encountered error.
  std::string description;
};

/// @relates drop_conn_msg
std::pair<ec, std::string_view> check(const drop_conn_msg& x);

/// @relates drop_conn_msg
template <class Inspector>
bool inspect(Inspector& f, drop_conn_msg& x) {
  return f.object(x).fields(f.field("magic", x.magic),
                            f.field("sender-id", x.sender_id),
                            f.field("code", x.code),
                            f.field("description", x.description));
}

/// @relates drop_conn_msg
inline drop_conn_msg make_drop_conn_msg(endpoint_id id, ec code,
                                        std::string description) {
  return {magic_number, id, static_cast<uint8_t>(code), std::move(description)};
}

// -- messages for the Broker protocol in version 1 ----------------------------

namespace v1 {

/// Announces filter to the responder.
struct originator_syn_msg {
  static constexpr auto tag = p2p_message_type::originator_syn;

  /// The filter of the originator.
  filter_type filter;
};

/// @relates originator_syn_msg
inline std::pair<ec, std::string_view> check(const originator_syn_msg&) {
  return {ec::none, {}};
}

/// @relates originator_syn_msg
template <class Inspector>
bool inspect(Inspector& f, originator_syn_msg& x) {
  return f.object(x).fields(f.field("filter", x.filter));
}

/// @relates originator_syn_msg
inline originator_syn_msg make_originator_syn_msg(filter_type filter) {
  return {std::move(filter)};
}

/// Announces filter to the originator.
struct responder_syn_ack_msg {
  static constexpr auto tag = p2p_message_type::responder_syn_ack;

  /// The filter of the responder.
  filter_type filter;
};

/// @relates responder_syn_ack_msg
inline std::pair<ec, std::string_view> check(const responder_syn_ack_msg&) {
  return {ec::none, {}};
}

/// @relates responder_syn_ack_msg
template <class Inspector>
bool inspect(Inspector& f, responder_syn_ack_msg& x) {
  return f.object(x).fields(f.field("filter", x.filter));
}

/// @relates responder_syn_ack_msg
inline responder_syn_ack_msg make_responder_syn_ack_msg(filter_type filter) {
  return {std::move(filter)};
}

/// Concludes the handshake. After this point, Broker may only send Phase 3
/// messages, i.e., @ref node_message.
struct originator_ack_msg {
  static constexpr auto tag = p2p_message_type::originator_ack;
};

/// @relates originator_ack
inline std::pair<ec, std::string_view> check(const originator_ack_msg&) {
  return {ec::none, {}};
}

/// @relates originator_ack
template <class Inspector>
bool inspect(Inspector& f, originator_ack_msg& x) {
  return f.object(x).fields();
}

/// @relates originator_ack_msg
inline originator_ack_msg make_originator_ack_msg() {
  return {};
}

/// Trait that translates between native @ref node_message and a binary network
/// representation.
class trait {
public:
  /// Serializes a @ref node_message to a sequence of bytes.
  bool convert(const node_message& msg, caf::byte_buffer& buf);

  /// Deserializes a @ref node_message from a sequence of bytes.
  bool convert(caf::const_byte_span bytes, node_message& msg);

  /// Retrieves the last error from a conversion.
  const caf::error& last_error() const noexcept {
    return last_error_;
  }

private:
  caf::error last_error_;
};

} // namespace v1

/// Wraps an error that occurred while parsing a @ref var_msg.
struct var_msg_error {
  ec code;
  std::string description;
};

/// Convenience type for handling phase 1 and phase 2 messages.
using var_msg =
  std::variant<var_msg_error, hello_msg, probe_msg, version_select_msg,
               drop_conn_msg, v1::originator_syn_msg, v1::responder_syn_ack_msg,
               v1::originator_ack_msg>;

/// @relates var_msg
std::string stringify(const var_msg& msg);

/// @relates var_msg
constexpr size_t hello_index = 1;

/// @relates var_msg
constexpr size_t probe_index = 2;

/// @relates var_msg
constexpr size_t version_select_index = 3;

/// @relates var_msg
constexpr size_t drop_conn_index = 4;

/// @relates var_msg
constexpr size_t originator_syn_index = 5;

/// @relates var_msg
constexpr size_t responder_syn_ack_index = 6;

/// @relates var_msg
constexpr size_t originator_ack_index = 7;

inline var_msg make_var_msg_error(ec code, std::string description) {
  return {var_msg_error{code, std::move(description)}};
}

/// Decodes a message from Broker's binary format.
var_msg decode(caf::const_byte_span bytes);

/// Encodes a message to Broker's binary format.
template <class Inspector, class T>
bool encode(Inspector& sink, const T& msg) {
  auto tag = T::tag;
  return sink.apply(tag) && sink.apply(msg);
}

} // namespace broker::internal::wire_format
