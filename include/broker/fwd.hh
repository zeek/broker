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

struct add_command;
struct clear_command;
struct endpoint_info;
struct enum_value;
struct erase_command;
struct expire_command;
struct network_info;
struct node_message;
struct none;
struct peer_info;
struct put_command;
struct put_unique_command;
struct set_command;
struct snapshot_command;
struct snapshot_sync_command;
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

// -- CAF type aliases ---------------------------------------------------------

using caf::optional;
using command_message = caf::cow_tuple<topic, internal_command>;
using data_message = caf::cow_tuple<topic, data>;
using node_message_content = caf::variant<data_message, command_message>;

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

class central_dispatcher;
class flare_actor;
class mailbox;
class unipath_manager;

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

#define BROKER_ADD_ATOM(name) CAF_ADD_ATOM(broker, broker::atom, name)

#define BROKER_ADD_TYPE_ID(type) CAF_ADD_TYPE_ID(broker, type)

CAF_BEGIN_TYPE_ID_BLOCK(broker, caf::first_custom_type_id)

  // -- atoms for generic communication ----------------------------------------

  BROKER_ADD_ATOM(ack)
  BROKER_ADD_ATOM(default_)
  BROKER_ADD_ATOM(id)
  BROKER_ADD_ATOM(init)
  BROKER_ADD_ATOM(name)
  BROKER_ADD_ATOM(network)
  BROKER_ADD_ATOM(peer)
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
  BROKER_ADD_ATOM(clear)
  BROKER_ADD_ATOM(clone)
  BROKER_ADD_ATOM(decrement)
  BROKER_ADD_ATOM(erase)
  BROKER_ADD_ATOM(exists)
  BROKER_ADD_ATOM(expire)
  BROKER_ADD_ATOM(increment)
  BROKER_ADD_ATOM(keys)
  BROKER_ADD_ATOM(local)
  BROKER_ADD_ATOM(master)
  BROKER_ADD_ATOM(mutable_check)
  BROKER_ADD_ATOM(resolve)
  BROKER_ADD_ATOM(restart)
  BROKER_ADD_ATOM(stale_check)
  BROKER_ADD_ATOM(store)
  BROKER_ADD_ATOM(subtract)
  BROKER_ADD_ATOM(sync_point)

  // -- atoms for communciation with the core actor ----------------------------

  BROKER_ADD_ATOM(no_events)
  BROKER_ADD_ATOM(snapshot)
  BROKER_ADD_ATOM(subscriptions)

  // -- Broker type announcements ----------------------------------------------

  BROKER_ADD_TYPE_ID((broker::add_command))
  BROKER_ADD_TYPE_ID((broker::address))
  BROKER_ADD_TYPE_ID((broker::backend))
  BROKER_ADD_TYPE_ID((broker::backend_options))
  BROKER_ADD_TYPE_ID((broker::clear_command))
  BROKER_ADD_TYPE_ID((broker::command_message))
  BROKER_ADD_TYPE_ID((broker::data))
  BROKER_ADD_TYPE_ID((broker::data_message))
  BROKER_ADD_TYPE_ID((broker::detail::retry_state))
  BROKER_ADD_TYPE_ID((broker::ec))
  BROKER_ADD_TYPE_ID((broker::endpoint_info))
  BROKER_ADD_TYPE_ID((broker::enum_value))
  BROKER_ADD_TYPE_ID((broker::erase_command))
  BROKER_ADD_TYPE_ID((broker::expire_command))
  BROKER_ADD_TYPE_ID((broker::filter_type))
  BROKER_ADD_TYPE_ID((broker::internal_command))
  BROKER_ADD_TYPE_ID((broker::network_info))
  BROKER_ADD_TYPE_ID((broker::node_message))
  BROKER_ADD_TYPE_ID((broker::node_message_content))
  BROKER_ADD_TYPE_ID((broker::none))
  BROKER_ADD_TYPE_ID((broker::optional<broker::timespan>))
  BROKER_ADD_TYPE_ID((broker::optional<broker::timestamp>))
  BROKER_ADD_TYPE_ID((broker::peer_info))
  BROKER_ADD_TYPE_ID((broker::port))
  BROKER_ADD_TYPE_ID((broker::put_command))
  BROKER_ADD_TYPE_ID((broker::put_unique_command))
  BROKER_ADD_TYPE_ID((broker::sc))
  BROKER_ADD_TYPE_ID((broker::set))
  BROKER_ADD_TYPE_ID((broker::set_command))
  BROKER_ADD_TYPE_ID((broker::snapshot))
  BROKER_ADD_TYPE_ID((broker::snapshot_command))
  BROKER_ADD_TYPE_ID((broker::snapshot_sync_command))
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
  BROKER_ADD_TYPE_ID((caf::stream<broker::node_message_content>))
  BROKER_ADD_TYPE_ID((std::vector<broker::command_message>))
  BROKER_ADD_TYPE_ID((std::vector<broker::data_message>))
  BROKER_ADD_TYPE_ID((std::vector<broker::node_message>))
  BROKER_ADD_TYPE_ID((std::vector<broker::node_message_content>))
  BROKER_ADD_TYPE_ID((std::vector<broker::peer_info>))

CAF_END_TYPE_ID_BLOCK(broker)

#undef BROKER_ADD_ATOM
#undef BROKER_ADD_TYPE_ID
