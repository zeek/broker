#pragma once

#include <cstdint>

#include <caf/cow_tuple.hpp>
#include <caf/variant.hpp>

#include "broker/alm/multipath.hh"
#include "broker/data.hh"
#include "broker/internal_command.hh"
#include "broker/topic.hh"

namespace broker {

/// A user-defined message with topic and data.
using data_message = caf::cow_tuple<topic, data>;

/// A broker-internal message with topic and command.
using command_message = caf::cow_tuple<topic, internal_command>;

using node_message_content = caf::variant<data_message, command_message>;

/// A message for node-to-node communication with either a user-defined data
/// message or a broker-internal command messages.
template <class PeerId>
using generic_node_message = caf::cow_tuple< // Fields:
  node_message_content,                      // 0: content
  alm::multipath<PeerId>,                    // 1: path
  std::vector<PeerId>                        // 2: receivers
  >;

/// A message for node-to-node communication with either a user-defined data
/// message or a broker-internal command messages.
using node_message = generic_node_message<caf::node_id>;

/// Returns whether `x` contains a ::node_message.
inline bool is_data_message(const node_message_content& x) {
  return caf::holds_alternative<data_message>(x);
}

/// Returns whether `x` contains a ::node_message.
template <class PeerId>
bool is_data_message(const generic_node_message<PeerId>& x) {
  return is_data_message(get<0>(x));
}

/// Returns whether `x` contains a ::command_message.
inline bool is_command_message(const node_message_content& x) {
  return caf::holds_alternative<command_message>(x);
}

/// Returns whether `x` contains a ::command_message.
template <class PeerId>
bool is_command_message(const generic_node_message<PeerId>& x) {
  return is_command_message(get<0>(x));
}

/// Generates a ::data_message.
template <class Topic, class Data>
data_message make_data_message(Topic&& t, Data&& d) {
  return data_message(std::forward<Topic>(t), std::forward<Data>(d));
}

/// Generates a ::command_message.
template <class Topic, class Command>
command_message make_command_message(Topic&& t, Command&& d) {
  return command_message(std::forward<Topic>(t), std::forward<Command>(d));
}

/// Generates a ::node_message.
template <class Value>
node_message make_node_message(Value&& value, alm::multipath<caf::node_id> path,
                               std::vector<caf::node_id> receivers = {}) {
  return node_message{std::forward<Value>(value), std::move(path),
                      std::move(receivers)};
}

/// Retrieves the topic from a ::data_message.
inline const topic& get_topic(const data_message& x) {
  return get<0>(x);
}

/// Retrieves the topic from a ::command_message.
inline const topic& get_topic(const command_message& x) {
  return get<0>(x);
}

/// Retrieves the topic from a ::generic_message.
inline const topic& get_topic(const node_message_content& x) {
  if (is_data_message(x))
    return get_topic(caf::get<data_message>(x));
  return get_topic(caf::get<command_message>(x));
}

/// Retrieves the topic from a ::generic_message.
template <class PeerId>
const topic& get_topic(const generic_node_message<PeerId>& x) {
  return get_topic(get<0>(x));
}

/// Moves the topic out of a ::data_message. Causes `x` to make a lazy copy of
/// its content if other ::data_message objects hold references to it.
inline topic&& move_topic(data_message& x) {
  return std::move(get<0>(x.unshared()));
}

/// Moves the topic out of a ::command_message. Causes `x` to make a lazy copy
/// of its content if other ::command_message objects hold references to it.
inline topic&& move_topic(command_message& x) {
  return std::move(get<0>(x.unshared()));
}

/// Moves the topic out of a ::node_message. Causes `x` to make a lazy copy of
/// its content if other ::node_message objects hold references to it.
inline topic&& move_topic(node_message_content& x) {
  if (is_data_message(x))
    return move_topic(caf::get<data_message>(x));
  return move_topic(caf::get<command_message>(x));
}

/// Moves the topic out of a ::node_message. Causes `x` to make a lazy copy of
/// its content if other ::node_message objects hold references to it.
template <class PeerId>
topic&& move_topic(generic_node_message<PeerId>& x) {
  return move_topic(get<0>(x));
}

/// Retrieves the data from a ::data_message.
inline const data& get_data(const data_message& x) {
  return get<1>(x);
}

/// Moves the data out of a ::data_message. Causes `x` to make a lazy copy of
/// its content if other ::data_message objects hold references to it.
inline data&& move_data(data_message& x) {
  return std::move(get<1>(x.unshared()));
}

/// Retrieves the command content from a ::command_message.
inline const internal_command::variant_type&
get_command(const command_message& x) {
  return get<1>(x).content;
}

/// Moves the command content out of a ::command_message. Causes `x` to make a
/// lazy copy of its content if other ::command_message objects hold references
/// to it.
inline internal_command::variant_type&& move_command(command_message& x) {
  return std::move(get<1>(x.unshared()).content);
}

/// Retrieves the content from a ::data_message.
template <class PeerId>
const node_message_content& get_content(const generic_node_message<PeerId>& x) {
  return get<0>(x);
}

/// Moves the content out of a ::node_message. Causes `x` to make a lazy copy of
/// its content if other ::node_message objects hold references to it.
template <class PeerId>
node_message_content&& move_content(generic_node_message<PeerId>& x) {
  return std::move(get<0>(x));
}

/// Retrieves the path from a ::data_message.
template <class PeerId>
const auto& get_path(const generic_node_message<PeerId>& x) {
  return get<1>(x);
}

/// Get unshared access the path field of a ::node_message. Causes `x` to make a
/// lazy copy of its content if other ::node_message objects hold references to
/// it.
template <class PeerId>
auto& get_unshared_path(generic_node_message<PeerId>& x) {
  return get<1>(x.unshared());
}

/// Retrieves the receivers from a ::data_message.
template <class PeerId>
const auto& get_receivers(const generic_node_message<PeerId>& x) {
  return get<2>(x);
}

/// Get unshared access the receivers field of a ::node_message. Causes `x` to
/// make a lazy copy of its content if other ::node_message objects hold
/// references to it.
template <class PeerId>
auto& get_unshared_receivers(generic_node_message<PeerId>& x) {
  return get<2>(x.unshared());
}

/// Shortcut for `get<data_message>(get_content(x))`.
template <class PeerId>
const data_message& get_data_message(const generic_node_message<PeerId>& x) {
  return get<data_message>(get_content(x));
}

/// Shortcut for `get<command_message>(get_content(x))`.
template <class PeerId>
const command_message&
get_command_message(const generic_node_message<PeerId>& x) {
  return get<command_message>(get_content(x));
}

} // namespace broker
