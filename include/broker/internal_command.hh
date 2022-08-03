#pragma once

#include <optional>
#include <unordered_map>
#include <utility>
#include <variant>

#include "broker/data.hh"
#include "broker/entity_id.hh"
#include "broker/fwd.hh"
#include "broker/snapshot.hh"
#include "broker/time.hh"
#include "broker/worker.hh"

namespace broker {

// -- meta information ---------------------------------------------------------

enum class command_tag {
  /// Identifies commands that represent an *action* on the data store. For
  /// example, adding or removing elements.
  action,
  /// Identifies control flow commands that a producer sends to its consumers.
  producer_control,
  /// Identifies control flow commands that a consumer sends to its producer.
  consumer_control,
};

std::string to_string(command_tag);

// -- broadcast: actions on the key-value store such as put and erase ----------

/// Sets a value in the key-value store.
struct put_command {
  data key;
  data value;
  std::optional<timespan> expiry;
  entity_id publisher;
  static constexpr auto tag = command_tag::action;
};

/// @relates put_command
template <class Inspector>
bool inspect(Inspector& f, put_command& x) {
  return f //
    .object(x)
    .pretty_name("put")
    .fields(f.field("key", x.key),       //
            f.field("value", x.value),   //
            f.field("expiry", x.expiry), //
            f.field("publisher", x.publisher));
}

/// Sets a value in the key-value store if its key does not already exist.
struct put_unique_command {
  data key;
  data value;
  std::optional<timespan> expiry;
  entity_id who;
  request_id req_id;
  entity_id publisher;
  static constexpr auto tag = command_tag::action;
};

/// @relates put_unique_command
template <class Inspector>
bool inspect(Inspector& f, put_unique_command& x) {
  return f //
    .object(x)
    .pretty_name("put_unique")
    .fields(f.field("key", x.key),       //
            f.field("value", x.value),   //
            f.field("expiry", x.expiry), //
            f.field("who", x.who),       //
            f.field("req_id", x.req_id), //
            f.field("publisher", x.publisher));
}

/// Sets a value in the key-value store if its key does not already exist.
struct put_unique_result_command {
  bool inserted;
  entity_id who;
  request_id req_id;
  entity_id publisher;
  static constexpr auto tag = command_tag::action;
};

/// @relates put_unique_result_command
template <class Inspector>
bool inspect(Inspector& f, put_unique_result_command& x) {
  return f //
    .object(x)
    .pretty_name("put_unique_result")
    .fields(f.field("inserted", x.inserted), //
            f.field("who", x.who),           //
            f.field("req_id", x.req_id),     //
            f.field("publisher", x.publisher));
}

/// Removes a value in the key-value store.
struct erase_command {
  data key;
  entity_id publisher;
  static constexpr auto tag = command_tag::action;
};

/// @relates erase_command
template <class Inspector>
bool inspect(Inspector& f, erase_command& x) {
  return f //
    .object(x)
    .pretty_name("erase")
    .fields(f.field("key", x.key), //
            f.field("publisher", x.publisher));
}

/// Removes a value in the key-value store as a result of an expiration. The
/// master sends this message type to the clones in order to allow them to
/// differentiate between a user actively removing an entry versus the master
/// removing it after expiration.
struct expire_command {
  data key;
  entity_id publisher;
  static constexpr auto tag = command_tag::action;
};

/// @relates expire_command
template <class Inspector>
bool inspect(Inspector& f, expire_command& x) {
  return f //
    .object(x)
    .pretty_name("expire")
    .fields(f.field("key", x.key), //
            f.field("publisher", x.publisher));
}

/// Adds a value to the existing value.
struct add_command {
  data key;
  data value;
  data::type init_type;
  std::optional<timespan> expiry;
  entity_id publisher;
  static constexpr auto tag = command_tag::action;
};

/// @relates add_command
template <class Inspector>
bool inspect(Inspector& f, add_command& x) {
  return f //
    .object(x)
    .pretty_name("add")
    .fields(f.field("key", x.key),             //
            f.field("value", x.value),         //
            f.field("init_type", x.init_type), //
            f.field("expiry", x.expiry),       //
            f.field("publisher", x.publisher));
}

/// Subtracts a value to the existing value.
struct subtract_command {
  data key;
  data value;
  std::optional<timespan> expiry;
  entity_id publisher;
  static constexpr auto tag = command_tag::action;
};

/// @relates subtract_command
template <class Inspector>
bool inspect(Inspector& f, subtract_command& x) {
  return f //
    .object(x)
    .pretty_name("subtract")
    .fields(f.field("key", x.key),       //
            f.field("value", x.value),   //
            f.field("expiry", x.expiry), //
            f.field("publisher", x.publisher));
}

/// Drops all values.
struct clear_command {
  entity_id publisher;
  static constexpr auto tag = command_tag::action;
};

/// @relates clear_command
template <class Inspector>
bool inspect(Inspector& f, clear_command& x) {
  return f.object(x)
    .pretty_name("clear") //
    .fields(f.field("publisher", x.publisher));
}

// -- unicast: one-to-one communication between clones and the master ----------

/// Causes the master to add a store writer to its list of inputs. Also acts as
/// handshake for the channel.
struct attach_writer_command {
  sequence_number_type offset;
  tick_interval_type heartbeat_interval;
  static constexpr auto tag = command_tag::producer_control;
};

/// @relates attach_writer_command
template <class Inspector>
bool inspect(Inspector& f, attach_writer_command& x) {
  return f //
    .object(x)
    .pretty_name("attach_writer")
    .fields(f.field("offset", x.offset), //
            f.field("heartbeat_interval", x.heartbeat_interval));
}

/// Confirms a clone and transfers the initial snapshot to a clone.
struct ack_clone_command {
  sequence_number_type offset;
  tick_interval_type heartbeat_interval;
  snapshot state;
  static constexpr auto tag = command_tag::producer_control;
};

/// @relates ack_clone_command
template <class Inspector>
bool inspect(Inspector& f, ack_clone_command& x) {
  return f //
    .object(x)
    .pretty_name("ack_clone")
    .fields(f.field("offset", x.offset),                         //
            f.field("heartbeat_interval", x.heartbeat_interval), //
            f.field("state", x.state));
}

/// Informs the receiver that the sender successfully handled all messages up to
/// a certain sequence number.
struct cumulative_ack_command {
  sequence_number_type seq;
  static constexpr auto tag = command_tag::consumer_control;
};

/// @relates cumulative_ack_command
template <class Inspector>
bool inspect(Inspector& f, cumulative_ack_command& x) {
  return f //
    .object(x)
    .pretty_name("cumulative_ack")
    .fields(f.field("seq", x.seq));
}

/// Informs the receiver that one or more commands failed to reach the sender.
struct nack_command {
  std::vector<sequence_number_type> seqs;
  static constexpr auto tag = command_tag::consumer_control;
};

/// @relates nack_command
template <class Inspector>
bool inspect(Inspector& f, nack_command& x) {
  return f //
    .object(x)
    .pretty_name("nack")
    .fields(f.field("seqs", x.seqs));
}

/// Informs all receivers that the sender is still alive.
struct keepalive_command {
  sequence_number_type seq;
  static constexpr auto tag = command_tag::producer_control;
};

/// @relates keepalive_command
template <class Inspector>
bool inspect(Inspector& f, keepalive_command& x) {
  return f //
    .object(x)
    .pretty_name("keepalive")
    .fields(f.field("seq", x.seq));
}

/// Notifies the receiver that the sender can no longer retransmit a command.
struct retransmit_failed_command {
  sequence_number_type seq;
  static constexpr auto tag = command_tag::producer_control;
};

/// @relates retransmit_failed_command
template <class Inspector>
bool inspect(Inspector& f, retransmit_failed_command& x) {
  return f //
    .object(x)
    .pretty_name("retransmit_failed")
    .fields(f.field("seq", x.seq));
}

// -- variant setup ------------------------------------------------------------

using internal_command_variant =
  std::variant<put_command, put_unique_command, put_unique_result_command,
               erase_command, expire_command, add_command, subtract_command,
               clear_command, attach_writer_command, keepalive_command,
               cumulative_ack_command, nack_command, ack_clone_command,
               retransmit_failed_command>;

class internal_command {
public:
  enum class type : uint8_t {
    put_command,
    put_unique_command,
    put_unique_result_command,
    erase_command,
    expire_command,
    add_command,
    subtract_command,
    clear_command,
    attach_writer_command,
    keepalive_command,
    cumulative_ack_command,
    nack_command,
    ack_clone_command,
    retransmit_failed_command,
  };

  /// A sender-specific sequence ID for establishing ordering on the messages.
  sequence_number_type seq;

  /// Encodes the sender of this command.
  entity_id sender;

  /// Encodes the designated receiver or `entity_id::nil` for broadcasted
  /// messages. Note: messages to the master are always "broadcasted", since
  /// there is only one master.
  entity_id receiver;

  /// Stores the content of the message.
  internal_command_variant content;
};

template <class Inspector>
bool inspect(Inspector& f, internal_command& x) {
  return f.object(x).fields(f.field("seq", x.seq), f.field("sender", x.sender),
                            f.field("receiver", x.receiver),
                            f.field("content", x.content));
}

} // namespace broker

namespace broker::detail {

constexpr command_tag command_tag_by_type[] = {
  put_command::tag,
  put_unique_command::tag,
  put_unique_result_command::tag,
  erase_command::tag,
  expire_command::tag,
  add_command::tag,
  subtract_command::tag,
  clear_command::tag,
  attach_writer_command::tag,
  keepalive_command::tag,
  cumulative_ack_command::tag,
  nack_command::tag,
  ack_clone_command::tag,
  retransmit_failed_command::tag,
};

inline command_tag tag_of(const internal_command_variant& x) {
  return command_tag_by_type[x.index()];
}

inline command_tag tag_of(const internal_command& cmd) {
  return command_tag_by_type[cmd.content.index()];
}

inline internal_command::type type_of(const internal_command& cmd) {
  return static_cast<internal_command::type>(cmd.content.index());
}

} // namespace broker::detail
