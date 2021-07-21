#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <set>
#include <unordered_map>
#include <vector>

#include <caf/allowed_unsafe_message_type.hpp>
#include <caf/config.hpp>
#include <caf/flow/fwd.hpp>
#include <caf/fwd.hpp>
#include <caf/type_id.hpp>
#include <caf/uuid.hpp>

namespace broker {

// -- PODs ---------------------------------------------------------------------

struct ack_clone_command;
struct add_command;
struct attach_clone_command;
struct attach_writer_command;
struct clear_command;
struct cumulative_ack_command;
struct domain_options;
struct endpoint_info;
struct entity_id;
struct enum_value;
struct erase_command;
struct expire_command;
struct keepalive_command;
struct nack_command;
struct network_info;
struct none;
struct peer_info;
struct put_command;
struct put_unique_command;
struct put_unique_result_command;
struct retransmit_failed_command;
struct subtract_command;

using publisher_id [[deprecated("use entity_id instead")]] = entity_id;

// -- classes ------------------------------------------------------------------

class address;
class configuration;
class data;
class endpoint;
class internal_command;
class port;
class publisher;
class shared_filter_type;
class shutdown_options;
class status;
class store;
class subnet;
class subscriber;
class topic;

// -- enum classes -------------------------------------------------------------

enum class backend : uint8_t;
enum class ec : uint8_t;
enum class sc : uint8_t;

// -- STD type aliases ---------------------------------------------------------

using backend_options = std::unordered_map<std::string, data>;
using clock = std::chrono::system_clock;
using filter_type = std::vector<topic>;
using set = std::set<data>;
using shared_filter_ptr = std::shared_ptr<shared_filter_type>;
using snapshot = std::unordered_map<data, data>;
using table = std::map<data, data>;
using timespan = std::chrono::duration<int64_t, std::nano>;
using timestamp = std::chrono::time_point<clock, timespan>;
using vector = std::vector<data>;

// -- arithmetic type aliases --------------------------------------------------

using boolean = bool;
using count = uint64_t;
using integer = int64_t;
using real = double;
using request_id = uint64_t;

} // namespace broker

// -- CAF type aliases (1) -----------------------------------------------------

namespace broker {

using caf::error;
using caf::optional;
using endpoint_id = caf::uuid;
using endpoint_id_list = std::vector<endpoint_id>;

} // namespace broker

// -- ALM types ----------------------------------------------------------------

namespace broker::alm {

class multipath;
class multipath_group;
class multipath_node;
class peer;
class routing_table_row;
class stream_transport;

struct endpoint_id_hasher;
struct lamport_timestamp;

using routing_table
  = std::unordered_map<endpoint_id, routing_table_row, endpoint_id_hasher>;

} // namespace broker::alm

// -- CAF type aliases (2) -----------------------------------------------------

namespace broker {

enum class packed_message_type : uint8_t;

using packed_message = caf::cow_tuple<packed_message_type, topic,
                                      std::vector<std::byte>>;
using command_message = caf::cow_tuple<topic, internal_command>;
using data_message = caf::cow_tuple<topic, data>;
using node_message = caf::cow_tuple<alm::multipath, packed_message>;

} // namespace broker

// -- Zeek interface types -----------------------------------------------------

namespace broker::zeek {

class Event;
class RelayEvent;
class HandleAndRelayEvent;
class LogCreate;
class LogWrite;
class IdentifierUpdate;

} // namespace broker::zeek

// -- private types ------------------------------------------------------------

namespace broker::detail {

struct command_message_publisher;
struct data_message_publisher;
struct packed_message_publisher;
struct retry_state;
struct store_state;

class connector;
class flare_actor;
class flow_controller;
class flow_controller_callback;
class mailbox;

template <class T>
class shared_publisher_queue;

template <class T>
class shared_subscriber_queue;

enum class item_scope : uint8_t;

enum class connector_event_id : uint64_t;

using connector_ptr = std::shared_ptr<connector>;
using store_state_ptr = std::shared_ptr<store_state>;
using weak_store_state_ptr = std::weak_ptr<store_state>;

using flow_controller_callback_ptr
  = caf::intrusive_ptr<flow_controller_callback>;

template <class T>
using shared_publisher_queue_ptr
  = caf::intrusive_ptr<shared_publisher_queue<T>>;

template <class T>
using shared_subscriber_queue_ptr
  = caf::intrusive_ptr<shared_subscriber_queue<T>>;

} // namespace broker::detail

// -- imported atoms -----------------------------------------------------------

#define BROKER_CAF_ATOM_ALIAS(name)                                            \
  using name = caf::name##_atom;                                               \
  constexpr auto name##_v = caf::name##_atom_v;

namespace broker::atom {

BROKER_CAF_ATOM_ALIAS(add)
BROKER_CAF_ATOM_ALIAS(connect)
BROKER_CAF_ATOM_ALIAS(get)
BROKER_CAF_ATOM_ALIAS(join)
BROKER_CAF_ATOM_ALIAS(leave)
BROKER_CAF_ATOM_ALIAS(ok)
BROKER_CAF_ATOM_ALIAS(publish)
BROKER_CAF_ATOM_ALIAS(put)
BROKER_CAF_ATOM_ALIAS(subscribe)
BROKER_CAF_ATOM_ALIAS(tick)
BROKER_CAF_ATOM_ALIAS(unsubscribe)
BROKER_CAF_ATOM_ALIAS(update)

} // namespace broker::atom

#undef BROKER_CAF_ATOM_ALIAS

// -- type announcements and custom atoms --------------------------------------

// Our type aliases for `timespan` and `timestamp` are identical to
// `caf::timespan` and `caf::timestamp`. Hence, these types should have a type
// ID assigned by CAF.

static_assert(caf::has_type_id<broker::timespan>::value,
              "broker::timespan != caf::timespan");

static_assert(caf::has_type_id<broker::timestamp>::value,
              "broker::timestamp != caf::timestamp");

#define BROKER_ADD_ATOM(...) CAF_ADD_ATOM(broker, broker::atom, __VA_ARGS__)

#define BROKER_ADD_TYPE_ID(type) CAF_ADD_TYPE_ID(broker, type)

CAF_BEGIN_TYPE_ID_BLOCK(broker, caf::first_custom_type_id)

  // -- atoms for generic communication ----------------------------------------

  BROKER_ADD_ATOM(ack)
  BROKER_ADD_ATOM(default_, "default")
  BROKER_ADD_ATOM(id)
  BROKER_ADD_ATOM(init)
  BROKER_ADD_ATOM(listen)
  BROKER_ADD_ATOM(name)
  BROKER_ADD_ATOM(network)
  BROKER_ADD_ATOM(peer)
  BROKER_ADD_ATOM(ping)
  BROKER_ADD_ATOM(pong)
  BROKER_ADD_ATOM(read)
  BROKER_ADD_ATOM(retry)
  BROKER_ADD_ATOM(run)
  BROKER_ADD_ATOM(shutdown)
  BROKER_ADD_ATOM(status)
  BROKER_ADD_ATOM(unpeer)
  BROKER_ADD_ATOM(write)

  // -- atoms for communication with workers -----------------------------------

  BROKER_ADD_ATOM(resume)

  // -- atoms for communication with stores ------------------------------------

  BROKER_ADD_ATOM(attach)
  BROKER_ADD_ATOM(await)
  BROKER_ADD_ATOM(clear)
  BROKER_ADD_ATOM(clone)
  BROKER_ADD_ATOM(decrement)
  BROKER_ADD_ATOM(erase)
  BROKER_ADD_ATOM(exists)
  BROKER_ADD_ATOM(expire)
  BROKER_ADD_ATOM(idle)
  BROKER_ADD_ATOM(increment)
  BROKER_ADD_ATOM(keys)
  BROKER_ADD_ATOM(local)
  BROKER_ADD_ATOM(master)
  BROKER_ADD_ATOM(mutable_check)
  BROKER_ADD_ATOM(resolve)
  BROKER_ADD_ATOM(restart)
  BROKER_ADD_ATOM(revoke)
  BROKER_ADD_ATOM(stale_check)
  BROKER_ADD_ATOM(store)
  BROKER_ADD_ATOM(subtract)
  BROKER_ADD_ATOM(sync_point)

  // -- atoms for communciation with the core actor ----------------------------

  BROKER_ADD_ATOM(no_events)
  BROKER_ADD_ATOM(snapshot)
  BROKER_ADD_ATOM(subscriptions)

  // -- Broker type announcements ----------------------------------------------

  BROKER_ADD_TYPE_ID((broker::ack_clone_command))
  BROKER_ADD_TYPE_ID((broker::add_command))
  BROKER_ADD_TYPE_ID((broker::address))
  BROKER_ADD_TYPE_ID((broker::alm::lamport_timestamp))
  BROKER_ADD_TYPE_ID((broker::alm::multipath))
  BROKER_ADD_TYPE_ID((broker::attach_clone_command))
  BROKER_ADD_TYPE_ID((broker::attach_writer_command))
  BROKER_ADD_TYPE_ID((broker::backend))
  BROKER_ADD_TYPE_ID((broker::backend_options))
  BROKER_ADD_TYPE_ID((broker::clear_command))
  BROKER_ADD_TYPE_ID((broker::command_message))
  BROKER_ADD_TYPE_ID((broker::cumulative_ack_command))
  BROKER_ADD_TYPE_ID((broker::data))
  BROKER_ADD_TYPE_ID((broker::data_message))
  BROKER_ADD_TYPE_ID((broker::detail::connector_event_id))
  BROKER_ADD_TYPE_ID((broker::detail::data_message_publisher))
  BROKER_ADD_TYPE_ID((broker::detail::flow_controller_callback_ptr))
  BROKER_ADD_TYPE_ID((broker::detail::packed_message_publisher))
  BROKER_ADD_TYPE_ID((broker::detail::retry_state))
  BROKER_ADD_TYPE_ID((broker::detail::store_state_ptr))
  BROKER_ADD_TYPE_ID((broker::ec))
  BROKER_ADD_TYPE_ID((broker::endpoint_info))
  BROKER_ADD_TYPE_ID((broker::entity_id))
  BROKER_ADD_TYPE_ID((broker::enum_value))
  BROKER_ADD_TYPE_ID((broker::erase_command))
  BROKER_ADD_TYPE_ID((broker::expire_command))
  BROKER_ADD_TYPE_ID((broker::filter_type))
  BROKER_ADD_TYPE_ID((broker::internal_command))
  BROKER_ADD_TYPE_ID((broker::keepalive_command))
  BROKER_ADD_TYPE_ID((broker::nack_command))
  BROKER_ADD_TYPE_ID((broker::network_info))
  BROKER_ADD_TYPE_ID((broker::node_message))
  BROKER_ADD_TYPE_ID((broker::none))
  BROKER_ADD_TYPE_ID((broker::optional<broker::timespan>))
  BROKER_ADD_TYPE_ID((broker::optional<broker::timestamp>))
  BROKER_ADD_TYPE_ID((broker::peer_info))
  BROKER_ADD_TYPE_ID((broker::port))
  BROKER_ADD_TYPE_ID((broker::put_command))
  BROKER_ADD_TYPE_ID((broker::put_unique_command))
  BROKER_ADD_TYPE_ID((broker::put_unique_result_command))
  BROKER_ADD_TYPE_ID((broker::retransmit_failed_command))
  BROKER_ADD_TYPE_ID((broker::sc))
  BROKER_ADD_TYPE_ID((broker::set))
  BROKER_ADD_TYPE_ID((broker::shutdown_options))
  BROKER_ADD_TYPE_ID((broker::snapshot))
  BROKER_ADD_TYPE_ID((broker::status))
  BROKER_ADD_TYPE_ID((broker::subnet))
  BROKER_ADD_TYPE_ID((broker::subtract_command))
  BROKER_ADD_TYPE_ID((broker::table))
  BROKER_ADD_TYPE_ID((broker::topic))
  BROKER_ADD_TYPE_ID((broker::vector))

  // -- STD/CAF type announcements ---------------------------------------------

  BROKER_ADD_TYPE_ID((caf::stream<broker::command_message>))
  BROKER_ADD_TYPE_ID((caf::stream<broker::data_message>))
  BROKER_ADD_TYPE_ID((caf::stream<broker::node_message>))
  BROKER_ADD_TYPE_ID((std::optional<broker::endpoint_id>))
  BROKER_ADD_TYPE_ID((std::vector<broker::alm::lamport_timestamp>))
  BROKER_ADD_TYPE_ID((std::vector<broker::command_message>))
  BROKER_ADD_TYPE_ID((std::vector<broker::data_message>))
  BROKER_ADD_TYPE_ID((std::vector<broker::endpoint_id>))
  BROKER_ADD_TYPE_ID((std::vector<broker::node_message>))
  BROKER_ADD_TYPE_ID((std::vector<broker::peer_info>))

#if CAF_VERSION < 1900
  BROKER_ADD_TYPE_ID((caf::uuid))
#endif

CAF_END_TYPE_ID_BLOCK(broker)

#undef BROKER_ADD_ATOM
#undef BROKER_ADD_TYPE_ID

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(broker::detail::command_message_publisher)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(broker::detail::data_message_publisher)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(broker::detail::flow_controller_callback_ptr)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(broker::detail::packed_message_publisher)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(broker::detail::retry_state)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(broker::detail::store_state_ptr)
