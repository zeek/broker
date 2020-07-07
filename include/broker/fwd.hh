#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <set>
#include <unordered_map>
#include <vector>

#include <caf/config.hpp>
#include <caf/fwd.hpp>
#include <caf/type_id.hpp>

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
struct peer_info;
struct put_command;
struct put_unique_command;
struct retransmit_failed_command;
struct subtract_command;

// -- classes ------------------------------------------------------------------

class address;
class configuration;
class data;
class endpoint;
class internal_command;
class port;
class publisher;
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

// -- ALM types ----------------------------------------------------------------

namespace broker::alm {

template <class PeerId>
class multipath;

struct lamport_timestamp;

} // namespace broker::alm

// -- CAF type aliases ---------------------------------------------------------

namespace broker {

using caf::optional;
using command_message = caf::cow_tuple<topic, internal_command>;
using data_message = caf::cow_tuple<topic, data>;
using endpoint_id = caf::node_id;
using node_message_content = caf::variant<data_message, command_message>;

template <class PeerId>
using generic_node_message
  = caf::cow_tuple<node_message_content, alm::multipath<PeerId>,
                   std::vector<PeerId>>;

using node_message = generic_node_message<caf::node_id>;

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

struct retry_state;

class flare_actor;
class mailbox;

} // namespace broker::detail

// -- imported atoms -----------------------------------------------------------

#define BROKER_CAF_ATOM_ALIAS(name)                                            \
  using name = caf::name##_atom;                                               \
  constexpr auto name##_v = caf::name##_atom_v;

namespace broker::atom{

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

// -- type announcements and custom atoms --------------------------------------

// Our type aliases for `timespan` and `timestamp` are identical to
// `caf::timespan` and `caf::timestamp`. Hence, these types should have a type
// ID assigned by CAF.

static_assert(caf::has_type_id<broker::timespan>::value,
              "broker::timespan != caf::timespan");

static_assert(caf::has_type_id<broker::timestamp>::value,
              "broker::timestamp != caf::timestamp");

#define BROKER_ADD_ATOM(name, text)                                            \
  CAF_ADD_ATOM(broker, broker::atom, name, text)

#define BROKER_ADD_TYPE_ID(type) CAF_ADD_TYPE_ID(broker, type)

CAF_BEGIN_TYPE_ID_BLOCK(broker, caf::first_custom_type_id)

  // -- atoms for generic communication ----------------------------------------

  BROKER_ADD_ATOM(ack, "ack")
  BROKER_ADD_ATOM(default_, "default")
  BROKER_ADD_ATOM(id, "id")
  BROKER_ADD_ATOM(init, "init")
  BROKER_ADD_ATOM(name, "name")
  BROKER_ADD_ATOM(network, "network")
  BROKER_ADD_ATOM(peer, "peer")
  BROKER_ADD_ATOM(read, "read")
  BROKER_ADD_ATOM(retry, "retry")
  BROKER_ADD_ATOM(run, "run")
  BROKER_ADD_ATOM(shutdown, "shutdown")
  BROKER_ADD_ATOM(status, "status")
  BROKER_ADD_ATOM(unpeer, "unpeer")
  BROKER_ADD_ATOM(write, "write")

  // -- atoms for communication with workers -----------------------------------

  BROKER_ADD_ATOM(resume, "resume")

  // -- atoms for communication with stores ------------------------------------

  BROKER_ADD_ATOM(attach, "attach")
  BROKER_ADD_ATOM(await, "await")
  BROKER_ADD_ATOM(clear, "clear")
  BROKER_ADD_ATOM(clone, "clone")
  BROKER_ADD_ATOM(decrement, "decrement")
  BROKER_ADD_ATOM(erase, "erase")
  BROKER_ADD_ATOM(exists, "exists")
  BROKER_ADD_ATOM(expire, "expire")
  BROKER_ADD_ATOM(idle, "idle")
  BROKER_ADD_ATOM(increment, "increment")
  BROKER_ADD_ATOM(keys, "keys")
  BROKER_ADD_ATOM(local, "local")
  BROKER_ADD_ATOM(master, "master")
  BROKER_ADD_ATOM(mutable_check, "mutable")
  BROKER_ADD_ATOM(resolve, "resolve")
  BROKER_ADD_ATOM(restart, "restart")
  BROKER_ADD_ATOM(revoke, "subtract")
  BROKER_ADD_ATOM(stale_check, "stale")
  BROKER_ADD_ATOM(store, "store")
  BROKER_ADD_ATOM(subtract, "subtract")
  BROKER_ADD_ATOM(sync_point, "sync_point")

  // -- atoms for communciation with the core actor ----------------------------

  BROKER_ADD_ATOM(no_events, "noEvents")
  BROKER_ADD_ATOM(snapshot, "snapshot")
  BROKER_ADD_ATOM(subscriptions, "subs")

  // -- Broker type announcements ----------------------------------------------

  BROKER_ADD_TYPE_ID((broker::ack_clone_command))
  BROKER_ADD_TYPE_ID((broker::add_command))
  BROKER_ADD_TYPE_ID((broker::address))
  BROKER_ADD_TYPE_ID((broker::alm::lamport_timestamp))
  BROKER_ADD_TYPE_ID((broker::attach_clone_command))
  BROKER_ADD_TYPE_ID((broker::attach_writer_command))
  BROKER_ADD_TYPE_ID((broker::backend))
  BROKER_ADD_TYPE_ID((broker::backend_options))
  BROKER_ADD_TYPE_ID((broker::clear_command))
  BROKER_ADD_TYPE_ID((broker::command_message))
  BROKER_ADD_TYPE_ID((broker::cumulative_ack_command))
  BROKER_ADD_TYPE_ID((broker::data))
  BROKER_ADD_TYPE_ID((broker::data_message))
  BROKER_ADD_TYPE_ID((broker::detail::retry_state))
  BROKER_ADD_TYPE_ID((broker::ec))
  BROKER_ADD_TYPE_ID((broker::endpoint_info))
  BROKER_ADD_TYPE_ID((broker::enum_value))
  BROKER_ADD_TYPE_ID((broker::entity_id))
  BROKER_ADD_TYPE_ID((broker::erase_command))
  BROKER_ADD_TYPE_ID((broker::expire_command))
  BROKER_ADD_TYPE_ID((broker::filter_type))
  BROKER_ADD_TYPE_ID((broker::internal_command))
  BROKER_ADD_TYPE_ID((broker::keepalive_command))
  BROKER_ADD_TYPE_ID((broker::nack_command))
  BROKER_ADD_TYPE_ID((broker::network_info))
  BROKER_ADD_TYPE_ID((broker::node_message))
  BROKER_ADD_TYPE_ID((broker::node_message_content))
  BROKER_ADD_TYPE_ID((broker::optional<broker::timespan>) )
  BROKER_ADD_TYPE_ID((broker::optional<broker::timestamp>) )
  BROKER_ADD_TYPE_ID((broker::peer_info))
  BROKER_ADD_TYPE_ID((broker::port))
  BROKER_ADD_TYPE_ID((broker::put_command))
  BROKER_ADD_TYPE_ID((broker::put_unique_command))
  BROKER_ADD_TYPE_ID((broker::retransmit_failed_command))
  BROKER_ADD_TYPE_ID((broker::sc))
  BROKER_ADD_TYPE_ID((broker::set))
  BROKER_ADD_TYPE_ID((broker::snapshot))
  BROKER_ADD_TYPE_ID((broker::status))
  BROKER_ADD_TYPE_ID((broker::subnet))
  BROKER_ADD_TYPE_ID((broker::subtract_command))
  BROKER_ADD_TYPE_ID((broker::table))
  BROKER_ADD_TYPE_ID((broker::topic))
  BROKER_ADD_TYPE_ID((broker::vector))

  // -- STD/CAF type announcements ---------------------------------------------

  BROKER_ADD_TYPE_ID((caf::stream<broker::command_message>) )
  BROKER_ADD_TYPE_ID((caf::stream<broker::data_message>) )
  BROKER_ADD_TYPE_ID((caf::stream<broker::node_message>) )
  BROKER_ADD_TYPE_ID((caf::stream<broker::node_message_content>) )
  BROKER_ADD_TYPE_ID((std::vector<broker::alm::lamport_timestamp>))
  BROKER_ADD_TYPE_ID((std::vector<broker::command_message>) )
  BROKER_ADD_TYPE_ID((std::vector<broker::data_message>) )
  BROKER_ADD_TYPE_ID((std::vector<broker::node_message>) )
  BROKER_ADD_TYPE_ID((std::vector<broker::node_message_content>) )
  BROKER_ADD_TYPE_ID((std::vector<broker::peer_info>) )
  BROKER_ADD_TYPE_ID((std::vector<caf::node_id>))

CAF_END_TYPE_ID_BLOCK(broker)

#undef BROKER_ADD_ATOM
#undef BROKER_ADD_TYPE_ID
