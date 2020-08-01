#pragma once

#include <utility>
#include <unordered_map>

#include <caf/actor.hpp>
#include <caf/variant.hpp>
#include <caf/optional.hpp>
#include <caf/meta/type_name.hpp>

#include "broker/data.hh"
#include "broker/detail/channel.hh"
#include "broker/entity_id.hh"
#include "broker/fwd.hh"
#include "broker/snapshot.hh"
#include "broker/time.hh"

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

// -- broadcast: operations on the key-value store such as put and erase -------

/// Adds a `publisher` field, tags the class as `action` command, and implements
/// an inspect overload.
#define BROKER_ACTION_COMMAND(name, ...)                                       \
  entity_id publisher;                                                         \
  static constexpr auto tag = command_tag::action;                             \
  template <class Inspector>                                                   \
  friend typename Inspector::result_type inspect(Inspector& f,                 \
                                                 name##_command& x) {          \
    auto& [__VA_ARGS__, publisher] = x;                                        \
    return f(caf::meta::type_name(#name), __VA_ARGS__, publisher);             \
  }

/// Sets a value in the key-value store.
struct put_command  {
  data key;
  data value;
  caf::optional<timespan> expiry;
  BROKER_ACTION_COMMAND(put, key, value, expiry)
};

/// Sets a value in the key-value store if its key does not already exist.
struct put_unique_command {
  data key;
  data value;
  caf::optional<timespan> expiry;
  entity_id who;
  request_id req_id;
  BROKER_ACTION_COMMAND(put_unique, key, value, expiry, who, req_id)
};

/// Sets a value in the key-value store if its key does not already exist.
struct put_unique_result_command {
  bool inserted;
  entity_id who;
  request_id req_id;
  BROKER_ACTION_COMMAND(put_unique_result, inserted, who, req_id)
};

/// Removes a value in the key-value store.
struct erase_command {
  data key;
  BROKER_ACTION_COMMAND(erase, key)
};

/// Removes a value in the key-value store as a result of an expiration. The
/// master sends this message type to the clones in order to allow them to
/// differentiate between a user actively removing an entry versus the master
/// removing it after expiration.
struct expire_command {
  data key;
  BROKER_ACTION_COMMAND(expire, key)
};

/// Adds a value to the existing value.
struct add_command {
  data key;
  data value;
  data::type init_type;
  caf::optional<timespan> expiry;
  BROKER_ACTION_COMMAND(add, key, value, init_type, expiry)
};

/// Subtracts a value to the existing value.
struct subtract_command {
  data key;
  data value;
  caf::optional<timespan> expiry;
  BROKER_ACTION_COMMAND(subtract, key, value, expiry)
};

/// Drops all values.
struct clear_command {
  entity_id publisher;
  static constexpr auto tag = command_tag::action;
  template <class Inspector>
  friend typename Inspector::result_type inspect(Inspector& f,
                                                 clear_command& x) {
    return f(caf::meta::type_name("clear"), x.publisher);
  }
};

#undef BROKER_ACTION_COMMAND

// -- unicast communication between clone and master ---------------------------

/// Tags the class as `control` command, and implements an inspect overload.
#define BROKER_CONTROL_COMMAND(origin, name, ...)                              \
  static constexpr auto tag = command_tag::origin##_control;                   \
  template <class Inspector>                                                   \
  friend typename Inspector::result_type inspect(Inspector& f,                 \
                                                 name##_command& x) {          \
    auto& [__VA_ARGS__] = x;                                                   \
    return f(caf::meta::type_name(#name), __VA_ARGS__);                        \
  }

/// Tags the class as `control` command, and implements an inspect overload.
#define BROKER_EMPTY_CONTROL_COMMAND(origin, name)                             \
  static constexpr auto tag = command_tag::origin##_control;                   \
  template <class Inspector>                                                   \
  friend typename Inspector::result_type inspect(Inspector& f,                 \
                                                 name##_command&) {            \
    return f(caf::meta::type_name(#name));                                     \
  }

/// Causes the master to add `remote_clone` to its list of clones.
struct attach_clone_command {
  BROKER_EMPTY_CONTROL_COMMAND(consumer, attach_clone)
};

/// Causes the master to add a store writer to its list of inputs. Also acts as
/// handshake for the channel.
struct attach_writer_command {
  detail::sequence_number_type offset;
  detail::tick_interval_type heartbeat_interval;
  BROKER_CONTROL_COMMAND(producer, attach_writer, offset, heartbeat_interval)
};

/// Confirms a clone and transfers the initial snapshot to a clone.
struct ack_clone_command {
  detail::sequence_number_type offset;
  detail::tick_interval_type heartbeat_interval;
  snapshot state;
  BROKER_CONTROL_COMMAND(producer, ack_clone, offset, heartbeat_interval, state)
};

/// Informs the receiver that the sender successfully handled all messages up to
/// a certain sequence number.
struct cumulative_ack_command {
  detail::sequence_number_type seq;
  BROKER_CONTROL_COMMAND(consumer, cumulative_ack, seq)
};

/// Informs the receiver that one or more commands failed to reach the sender.
struct nack_command {
  std::vector<detail::sequence_number_type> seqs;
  BROKER_CONTROL_COMMAND(consumer, nack, seqs)
};

/// Causes the master to remove a clone.
struct remove_clone_command {
  BROKER_EMPTY_CONTROL_COMMAND(consumer, remove_clone)
};

/// Causes a store writer to remove the master.
struct remove_reader_command {
  BROKER_EMPTY_CONTROL_COMMAND(consumer, remove_reader)
};

/// Informs all receivers that the master is still alive.
struct keepalive_command {
  detail::sequence_number_type seq;
  BROKER_CONTROL_COMMAND(producer, keepalive, seq)
};

/// Notifies the receiver that the sender can no longer retransmit a command.
struct retransmit_failed_command {
  detail::sequence_number_type seq;
  BROKER_CONTROL_COMMAND(producer, retransmit_failed, seq)
};

/// Notifies all receivers that a writer has shut down.
struct writer_shutdown_command {
  detail::sequence_number_type final_seq;
  BROKER_CONTROL_COMMAND(producer, writer_shutdown, final_seq)
};

/// Notifies all receivers that the master has shut down.
struct master_shutdown_command {
  detail::sequence_number_type final_seq;
  BROKER_CONTROL_COMMAND(producer, master_shutdown, final_seq)
};

#undef BROKER_CONTROL_COMMAND
#undef BROKER_EMPTY_CONTROL_COMMAND

// -- variant setup ------------------------------------------------------------

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
    attach_clone_command,
    attach_writer_command,
    keepalive_command,
    cumulative_ack_command,
    nack_command,
    remove_clone_command,
    remove_reader_command,
    ack_clone_command,
    retransmit_failed_command,
    writer_shutdown_command,
    master_shutdown_command,
  };

  using variant_type
    = caf::variant<put_command, put_unique_command, put_unique_result_command,
                   erase_command, expire_command, add_command, subtract_command,
                   clear_command, attach_clone_command, attach_writer_command,
                   keepalive_command, cumulative_ack_command, nack_command,
                   remove_clone_command, remove_reader_command,
                   ack_clone_command, retransmit_failed_command,
                   writer_shutdown_command, master_shutdown_command>;

  detail::sequence_number_type seq;

  entity_id sender;

  variant_type content;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, internal_command& x) {
  return f(caf::meta::type_name("internal_command"), x.seq, x.sender,
           x.content);
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
  attach_clone_command::tag,
  attach_writer_command::tag,
  keepalive_command::tag,
  cumulative_ack_command::tag,
  nack_command::tag,
  remove_clone_command::tag,
  remove_reader_command::tag,
  ack_clone_command::tag,
  retransmit_failed_command::tag,
  writer_shutdown_command::tag,
  master_shutdown_command::tag,
};

inline command_tag tag_of(const internal_command& cmd) {
  return command_tag_by_type[cmd.content.index()];
}

inline internal_command::type type_of(const internal_command& cmd) {
  return static_cast<internal_command::type>(cmd.content.index());
}

} // namespace broker::detail
