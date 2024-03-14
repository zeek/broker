#pragma once

#include "broker/command_envelope.hh"
#include "broker/data_envelope.hh"
#include "broker/detail/inspect_enum.hh"
#include "broker/envelope.hh"
#include "broker/intrusive_ptr.hh"
#include "broker/p2p_message_type.hh"
#include "broker/ping_envelope.hh"
#include "broker/pong_envelope.hh"
#include "broker/routing_update_envelope.hh"
#include "broker/topic.hh"
#include "broker/variant.hh"

// -- backwards compatibility API ----------------------------------------------

namespace broker {

using node_message = envelope_ptr;

template <class T, class = std::enable_if_t<std::is_base_of_v<envelope, T>>>
auto get_topic(const intrusive_ptr<T>& msg) {
  return msg->topic();
}

template <class T, class = std::enable_if_t<std::is_base_of_v<envelope, T>>>
auto get_topic_str(const intrusive_ptr<T>& msg) {
  return msg->topic();
}

template <class T, class = std::enable_if_t<std::is_base_of_v<envelope, T>>>
auto get_sender(const intrusive_ptr<T>& msg) {
  return msg->sender();
}

template <class T, class = std::enable_if_t<std::is_base_of_v<envelope, T>>>
auto get_receiver(const intrusive_ptr<T>& msg) {
  return msg->receiver();
}

template <class T, class = std::enable_if_t<std::is_base_of_v<envelope, T>>>
auto get_type(const intrusive_ptr<T>& msg) {
  return msg->type();
}

using data_message = data_envelope_ptr;

template <class... Ts>
auto make_data_message(Ts&&... args) {
  return data_envelope::make(std::forward<Ts>(args)...);
}

inline variant get_data(const data_message& msg) {
  return msg->value();
}

inline variant move_data(const data_message& msg) {
  return msg->value();
}

using command_message = command_envelope_ptr;

template <class... Ts>
auto make_command_message(Ts&&... args) {
  return command_envelope::make(std::forward<Ts>(args)...);
}

inline const internal_command& get_command(const command_message& msg) {
  return msg->value();
}

using ping_message = ping_envelope_ptr;

template <class... Ts>
auto make_ping_message(Ts&&... args) {
  return ping_envelope::make(std::forward<Ts>(args)...);
}

using pong_message = pong_envelope_ptr;

template <class... Ts>
auto make_pong_message(Ts&&... args) {
  return pong_envelope::make(std::forward<Ts>(args)...);
}

using packed_message_type = envelope_type;

template <class T>
constexpr packed_message_type packed_type() {
  if constexpr (std::is_same_v<T, data_message>) {
    return envelope_type::data;
  } else {
    static_assert(std::is_same_v<T, command_message>);
    return envelope_type::command;
  }
}

} // namespace broker
