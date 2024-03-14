#pragma once

#include "broker/detail/inspect_enum.hh"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

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

} // namespace broker
