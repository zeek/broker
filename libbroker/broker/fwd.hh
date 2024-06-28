#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

namespace broker {

// -- PODs ---------------------------------------------------------------------

struct broker_options;
struct domain_options;
struct endpoint_info;
struct entity_id;
struct enum_value;
struct lamport_timestamp;
struct network_info;
struct none;
struct peer_info;

// -- internal command types ---------------------------------------------------

struct put_command;
struct put_unique_command;
struct put_unique_result_command;
struct erase_command;
struct expire_command;
struct add_command;
struct subtract_command;
struct clear_command;
struct attach_writer_command;
struct ack_clone_command;
struct cumulative_ack_command;
struct nack_command;
struct keepalive_command;
struct retransmit_failed_command;

using publisher_id [[deprecated("use entity_id instead")]] = entity_id;

// -- classes ------------------------------------------------------------------

class address;
class command_envelope;
class configuration;
class data;
class data_envelope;
class endpoint;
class endpoint_id;
class enum_value_view;
class envelope;
class error;
class internal_command;
class list_builder;
class mailbox;
class ping_envelope;
class pong_envelope;
class port;
class publisher;
class routing_update_envelope;
class set_builder;
class shared_filter_type;
class shutdown_options;
class status;
class store;
class subnet;
class subscriber;
class table_builder;
class topic;
class variant;
class variant_data;
class variant_list;
class variant_set;
class variant_table;
class worker;

// -- templates ----------------------------------------------------------------

template <class T>
class expected;

template <class... Ts>
class cow_tuple;

template <class T>
class intrusive_ptr;

// -- enum classes -------------------------------------------------------------

enum class backend : uint8_t;
enum class ec : uint8_t;
enum class p2p_message_type : uint8_t;
enum class sc : uint8_t;
enum class variant_tag : uint8_t;

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

using internal_command_variant =
  std::variant<put_command, put_unique_command, put_unique_result_command,
               erase_command, expire_command, add_command, subtract_command,
               clear_command, attach_writer_command, keepalive_command,
               cumulative_ack_command, nack_command, ack_clone_command,
               retransmit_failed_command>;

// -- arithmetic type aliases --------------------------------------------------

using boolean = bool;
using count = uint64_t;
using integer = int64_t;
using real = double;
using request_id = uint64_t;

/// Integer type for the monotonically increasing counters large enough to
/// neglect wraparounds. At 1000 messages per second, a sequence number of this
/// type overflows after 580 *million* years.
using sequence_number_type = uint64_t;

/// Integer type for measuring configurable intervals in ticks.
using tick_interval_type = uint16_t;

} // namespace broker

// -- ALM types ----------------------------------------------------------------

namespace broker::alm {

class multipath;
class multipath_group;
class multipath_node;
class routing_table_row;

using routing_table = std::unordered_map<endpoint_id, routing_table_row>;

} // namespace broker::alm

// -- message type aliases -----------------------------------------------------

namespace broker {

using command_envelope_ptr = intrusive_ptr<const command_envelope>;
using data_envelope_ptr = intrusive_ptr<const data_envelope>;
using envelope_ptr = intrusive_ptr<const envelope>;
using ping_envelope_ptr = intrusive_ptr<const ping_envelope>;
using pong_envelope_ptr = intrusive_ptr<const pong_envelope>;
using routing_update_envelope_ptr =
  intrusive_ptr<const routing_update_envelope>;

// Backwards compatibility.
using node_message = envelope_ptr;
using data_message = data_envelope_ptr;
using command_message = command_envelope_ptr;
using ping_message = ping_envelope_ptr;
using pong_message = pong_envelope_ptr;

} // namespace broker

// -- implementation details ---------------------------------------------------

namespace broker::detail {

class abstract_backend;
class monotonic_buffer_resource;

} // namespace broker::detail

// -- Zeek interface types -----------------------------------------------------

namespace broker::zeek {

class Event;
class RelayEvent;
class HandleAndRelayEvent;
class LogCreate;
class LogWrite;
class IdentifierUpdate;

} // namespace broker::zeek

// -- third-party types --------------------------------------------------------

namespace prometheus {
class Counter;
class Gauge;
class Historgram;
class Registry;
} // namespace prometheus

// -- type aliases for third-party libraries -----------------------------------

namespace broker {

using prometheus_registry_ptr = std::shared_ptr<prometheus::Registry>;

} // namespace broker
