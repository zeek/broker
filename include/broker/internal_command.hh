#pragma once

#include <utility>
#include <unordered_map>

#include <caf/actor.hpp>
#include <caf/variant.hpp>
#include <caf/optional.hpp>

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

// -- utility for BROKER_ACTION_COMMAND ----------------------------------------

#define BROKER_PP_EXPAND(...) __VA_ARGS__

namespace detail {

template <class Inspector, class T, size_t N, class Tuple, size_t... Is>
bool inspect_impl(Inspector& f, T& obj, caf::string_view pretty_name,
                  caf::string_view (&names)[N], Tuple refs,
                  std::index_sequence<Is...>) {
  static_assert(N == sizeof...(Is));
  return f.object(obj)
    .pretty_name(pretty_name)
    .fields(f.field(names[Is], std::get<Is>(refs))...);
}

} // namespace detail

// -- broadcast: operations on the key-value store such as put and erase -------

/// Adds a `publisher` field, tags the class as `action` command, and implements
/// an inspect overload.
#define BROKER_ACTION_COMMAND(name, field_names_pack, ...)                     \
  entity_id publisher;                                                         \
  static constexpr auto tag = command_tag::action;                             \
  template <class Inspector>                                                   \
  friend bool inspect(Inspector& f, name##_command& x) {                       \
    auto& [__VA_ARGS__, publisher] = x;                                        \
    caf::string_view field_names[] = {                                         \
      BROKER_PP_EXPAND field_names_pack,                                       \
      "publisher",                                                             \
    };                                                                         \
    auto refs = std::forward_as_tuple(__VA_ARGS__, publisher);                 \
    std::make_index_sequence<std::tuple_size<decltype(refs)>::value> iseq;     \
    return detail::inspect_impl(f, x, #name, field_names, refs, iseq);         \
  }

/// Sets a value in the key-value store.
struct put_command  {
  data key;
  data value;
  caf::optional<timespan> expiry;
  BROKER_ACTION_COMMAND(put, ("key", "value", "expiry"), key, value, expiry)
};

/// Sets a value in the key-value store if its key does not already exist.
struct put_unique_command {
  data key;
  data value;
  caf::optional<timespan> expiry;
  entity_id who;
  request_id req_id;
  BROKER_ACTION_COMMAND(put_unique, ("key", "value", "expiry", "who", "req_id"),
                        key, value, expiry, who, req_id)
};

/// Sets a value in the key-value store if its key does not already exist.
struct put_unique_result_command {
  bool inserted;
  entity_id who;
  request_id req_id;
  BROKER_ACTION_COMMAND(put_unique_result, ("inserted", "who", "req_id"),
                        inserted, who, req_id)
};

/// Removes a value in the key-value store.
struct erase_command {
  data key;
  BROKER_ACTION_COMMAND(erase, ("key"), key)
};

/// Removes a value in the key-value store as a result of an expiration. The
/// master sends this message type to the clones in order to allow them to
/// differentiate between a user actively removing an entry versus the master
/// removing it after expiration.
struct expire_command {
  data key;
  BROKER_ACTION_COMMAND(expire, ("key"), key)
};

/// Adds a value to the existing value.
struct add_command {
  data key;
  data value;
  data::type init_type;
  caf::optional<timespan> expiry;
  BROKER_ACTION_COMMAND(add, ("key", "value", "init_type", "expiry"), key,
                        value, init_type, expiry)
};

/// Subtracts a value to the existing value.
struct subtract_command {
  data key;
  data value;
  caf::optional<timespan> expiry;
  BROKER_ACTION_COMMAND(subtract, ("key", "value", "expiry"), key, value,
                        expiry)
};

/// Drops all values.
struct clear_command {
  entity_id publisher;
  static constexpr auto tag = command_tag::action;
  template <class Inspector>
  friend bool inspect(Inspector& f, clear_command& x) {
    return f.object(x)
      .pretty_name("clear") //
      .fields(f.field("publisher", x.publisher));
  }
};

#undef BROKER_ACTION_COMMAND

// -- unicast communication between clone and master ---------------------------

/// Tags the class as `control` command, and implements an inspect overload.
#define BROKER_CONTROL_COMMAND(origin, name, field_names_pack, ...)            \
  static constexpr auto tag = command_tag::origin##_control;                   \
  template <class Inspector>                                                   \
  friend bool inspect(Inspector& f, name##_command& x) {                       \
    auto& [__VA_ARGS__] = x;                                                   \
    caf::string_view field_names[] = {BROKER_PP_EXPAND field_names_pack};      \
    auto refs = std::forward_as_tuple(__VA_ARGS__);                            \
    std::make_index_sequence<std::tuple_size<decltype(refs)>::value> iseq;     \
    return detail::inspect_impl(f, x, #name, field_names, refs, iseq);         \
  }

/// Causes the master to add `remote_clone` to its list of clones.
struct attach_clone_command {
  static constexpr auto tag = command_tag::consumer_control;

  template <class Inspector>
  friend bool inspect(Inspector& f, attach_clone_command& x) {
    return f.object(x).pretty_name("attach_clone").fields();
  }
};

/// Causes the master to add a store writer to its list of inputs. Also acts as
/// handshake for the channel.
struct attach_writer_command {
  detail::sequence_number_type offset;
  detail::tick_interval_type heartbeat_interval;
  BROKER_CONTROL_COMMAND(producer, attach_writer,
                         ("offset", "heartbeat_interval"), offset,
                         heartbeat_interval)
};

/// Confirms a clone and transfers the initial snapshot to a clone.
struct ack_clone_command {
  detail::sequence_number_type offset;
  detail::tick_interval_type heartbeat_interval;
  snapshot state;
  BROKER_CONTROL_COMMAND(producer, ack_clone,
                         ("offset", "heartbeat_interval", "state"), offset,
                         heartbeat_interval, state)
};

/// Informs the receiver that the sender successfully handled all messages up to
/// a certain sequence number.
struct cumulative_ack_command {
  detail::sequence_number_type seq;
  BROKER_CONTROL_COMMAND(consumer, cumulative_ack, ("seq"), seq)
};

/// Informs the receiver that one or more commands failed to reach the sender.
struct nack_command {
  std::vector<detail::sequence_number_type> seqs;
  BROKER_CONTROL_COMMAND(consumer, nack, ("seqs"), seqs)
};

/// Informs all receivers that the sender is still alive.
struct keepalive_command {
  detail::sequence_number_type seq;
  BROKER_CONTROL_COMMAND(producer, keepalive, ("seq"), seq)
};

/// Notifies the receiver that the sender can no longer retransmit a command.
struct retransmit_failed_command {
  detail::sequence_number_type seq;
  BROKER_CONTROL_COMMAND(producer, retransmit_failed, ("seq"), seq)
};

#undef BROKER_CONTROL_COMMAND

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
    ack_clone_command,
    retransmit_failed_command,
  };

  using variant_type
    = caf::variant<put_command, put_unique_command, put_unique_result_command,
                   erase_command, expire_command, add_command, subtract_command,
                   clear_command, attach_clone_command, attach_writer_command,
                   keepalive_command, cumulative_ack_command, nack_command,
                   ack_clone_command, retransmit_failed_command>;

  detail::sequence_number_type seq;

  entity_id sender;

  variant_type content;
};

template <class Inspector>
bool inspect(Inspector& f, internal_command& x) {
  return f.object(x).fields(f.field("seq", x.seq), f.field("sender", x.sender),
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
  attach_clone_command::tag,
  attach_writer_command::tag,
  keepalive_command::tag,
  cumulative_ack_command::tag,
  nack_command::tag,
  ack_clone_command::tag,
  retransmit_failed_command::tag,
};

inline command_tag tag_of(const internal_command& cmd) {
  return command_tag_by_type[cmd.content.index()];
}

inline internal_command::type type_of(const internal_command& cmd) {
  return static_cast<internal_command::type>(cmd.content.index());
}

} // namespace broker::detail
