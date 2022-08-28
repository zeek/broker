#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "broker/cow_tuple.hh"
#include "broker/data.hh"
#include "broker/detail/inspect_enum.hh"
#include "broker/internal_command.hh"
#include "broker/topic.hh"

namespace broker {

/// Tags a peer-to-peer message with the type of the serialized payload.
enum class p2p_message_type : uint8_t {
  data = 1,          ///< Payload contains a @ref data_message.
  command,           ///< Payload contains a @ref command_message.
  routing_update,    ///< Payload contains a flooded update.
  ping,              ///< Connectivity checking after the handshake.
  pong,              ///< The response to a `ping` message.
  hello,             ///< Starts the handshake process.
  probe,             ///< Probing of connectivity without other effects.
  version_select,    ///< Selects the version for all future messages.
  drop_conn,         ///< Aborts the handshake with an error.
  originator_syn,    ///< Ship filter and local time from orig to resp.
  responder_syn_ack, ///< Ship filter and local time from resp to orig.
  originator_ack,    ///< Finalizes the peering process.
};

/// @relates p2p_message_type
std::string to_string(p2p_message_type);

/// @relates p2p_message_type
bool from_string(std::string_view, p2p_message_type&);

/// @relates p2p_message_type
bool from_integer(uint8_t, p2p_message_type&);

/// @relates p2p_message_type
template <class Inspector>
bool inspect(Inspector& f, p2p_message_type& x) {
  return detail::inspect_enum(f, x);
}

/// Tags a packed message with the type of the serialized data. This enumeration
/// is a subset of @ref p2p_message_type.
enum class packed_message_type : uint8_t {
  data = 1,
  command,
  routing_update,
  ping,
  pong,
};

/// @relates packed_message_type
std::string to_string(packed_message_type);

/// @relates packed_message_type
bool from_string(std::string_view, packed_message_type&);

/// @relates packed_message_type
bool from_integer(uint8_t, packed_message_type&);

/// @relates packed_message_type
template <class Inspector>
bool inspect(Inspector& f, packed_message_type& x) {
  return detail::inspect_enum(f, x);
}

/// A Broker-internal message with a payload received from the ALM layer.
using packed_message =
  cow_tuple<packed_message_type, uint16_t, topic, std::vector<std::byte>>;

/// @relates packed_message
inline packed_message make_packed_message(packed_message_type type,
                                          uint16_t ttl, topic dst,
                                          std::vector<std::byte> bytes) {
  return packed_message{type, ttl, std::move(dst), std::move(bytes)};
}

/// @relates packed_message
template <class T>
inline packed_message make_packed_message(packed_message_type type,
                                          uint16_t ttl, topic dst,
                                          const std::vector<T>& buf) {
  static_assert(sizeof(T) == 1);
  auto first = reinterpret_cast<const std::byte*>(buf.data());
  auto last = first + buf.size();
  return packed_message{type, ttl, std::move(dst),
                        std::vector<std::byte>{first, last}};
}

/// @relates packed_message
inline packed_message_type get_type(const packed_message& msg) {
  return get<0>(msg);
}

/// @relates packed_message
inline uint16_t get_ttl(const packed_message& msg) {
  return get<1>(msg);
}

/// @relates packed_message
inline const topic& get_topic(const packed_message& msg) {
  return get<2>(msg);
}

/// @relates packed_message
inline const std::vector<std::byte>& get_payload(const packed_message& msg) {
  return get<3>(msg);
}

/// A Broker-internal message with path and content (packed message).
using node_message = cow_tuple<endpoint_id,     // Sender.
                               endpoint_id,     // Receiver or NIL.
                               packed_message>; // Content.

/// @relates node_message
inline auto get_sender(const node_message& msg) {
  return get<0>(msg);
}

/// @relates node_message
inline auto get_receiver(const node_message& msg) {
  return get<1>(msg);
}

/// @relates node_message
inline const packed_message& get_packed_message(const node_message& msg) {
  return get<2>(msg);
}

/// @relates node_message
inline auto get_ttl(const node_message& msg) {
  return get_ttl(get_packed_message(msg));
}

/// @relates node_message
inline auto get_type(const node_message& msg) {
  return get_type(get_packed_message(msg));
}

/// @relates node_message
inline const topic& get_topic(const node_message& msg) {
  return get_topic(get_packed_message(msg));
}

/// @relates node_message
inline const std::vector<std::byte>& get_payload(const node_message& msg) {
  return get_payload(get_packed_message(msg));
}

/// A user-defined message with topic and data.
using data_message = cow_tuple<topic, data>;

/// A Broker-internal message with topic and command.
using command_message = cow_tuple<topic, internal_command>;

/// A Broker-internal message for testing connectivity.
using ping_message = cow_tuple<std::vector<std::byte>>;

/// A Broker-internal message for testing connectivity.
using pong_message = cow_tuple<std::vector<std::byte>>;

/// Helper class for implementing @ref packed_message_type_v.
template <class T>
struct packed_message_type_oracle;

template <>
struct packed_message_type_oracle<data_message> {
  static constexpr auto value = packed_message_type::data;
};

template <>
struct packed_message_type_oracle<command_message> {
  static constexpr auto value = packed_message_type::command;
};

/// Translates a type to its corresponding @ref packed_message_type value.
template <class T>
constexpr auto packed_message_type_v = packed_message_type_oracle<T>::value;

/// Generates a @ref data_message.
template <class Topic, class Data>
data_message make_data_message(Topic&& t, Data&& d) {
  return data_message(std::forward<Topic>(t), std::forward<Data>(d));
}

/// Generates a @ref command_message.
template <class Topic, class Command>
command_message make_command_message(Topic&& t, Command&& d) {
  return command_message(std::forward<Topic>(t), std::forward<Command>(d));
}

/// Generates a @ref node_message with NIL receiver, causing all receivers to
/// dispatch on topic only.
inline node_message make_node_message(endpoint_id sender, packed_message pm) {
  return node_message{sender, endpoint_id::nil(), std::move(pm)};
}

/// Generates a @ref node_message.
inline node_message make_node_message(endpoint_id sender, endpoint_id receiver,
                                      packed_message pm) {
  return node_message{sender, receiver, std::move(pm)};
}

/// Retrieves the topic from a @ref data_message.
/// @relates data_message
inline const topic& get_topic(const data_message& x) {
  return get<0>(x);
}

/// Retrieves the topic from a ::command_message.
/// @relates data_message
inline const topic& get_topic(const command_message& x) {
  return get<0>(x);
}

/// Moves the topic out of a ::data_message. Causes `x` to make a lazy copy of
/// its content if other ::data_message objects hold references to it.
/// @relates data_message
inline topic&& move_topic(data_message& x) {
  return std::move(get<0>(x.unshared()));
}

/// Moves the topic out of a ::command_message. Causes `x` to make a lazy copy
/// of its content if other ::command_message objects hold references to it.
/// @relates data_message
inline topic&& move_topic(command_message& x) {
  return std::move(get<0>(x.unshared()));
}

/// Retrieves the data from a @ref data_message.
/// @relates data_message
inline const data& get_data(const data_message& x) {
  return get<1>(x);
}

/// Moves the data out of a @ref data_message. Causes `x` to make a lazy copy of
/// its content if other @ref data_message objects hold references to it.
/// @relates data_message
inline data&& move_data(data_message& x) {
  return std::move(get<1>(x.unshared()));
}

/// Retrieves the command content from a ::command_message.
/// @relates command_message
inline const internal_command& get_command(const command_message& x) {
  return get<1>(x);
}

/// Moves the command content out of a ::command_message. Causes `x` to make a
/// lazy copy of its content if other ::command_message objects hold references
/// to it.
/// @relates command_message
inline internal_command&& move_command(command_message& x) {
  return std::move(get<1>(x.unshared()));
}

/// Force `x` to become uniquely referenced. Performs a deep-copy of the content
/// in case they is more than one reference to it. If `x` is the only object
/// referring to the content, this function does nothing.
inline void force_unshared(data_message& x) {
  x.unshared();
}

/// @copydoc force_unshared
inline void force_unshared(command_message& x) {
  x.unshared();
}

/// Converts `msg` to a human-readable string representation.
/// @relates data_message
std::string to_string(const data_message& msg);

/// Converts `msg` to a human-readable string representation.
/// @relates command_message
std::string to_string(const command_message& msg);

/// Gives data messages a nicer representation in JSON input and output.
struct data_message_decorator {
  topic& t;
  data& d;
};

/// @relates data_message_decorator
template <class Inspector>
bool inspect(Inspector& f, data_message_decorator& x) {
  return f.object(x).fields(f.field("topic", x.t),
                            f.field("data", x.d.get_data()));
}

/// @relates data_message
inline data_message_decorator decorated(data_message& msg) {
  auto& [t, d] = msg.unshared();
  return data_message_decorator{t, d};
}

} // namespace broker
