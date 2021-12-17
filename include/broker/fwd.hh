#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <set>
#include <unordered_map>
#include <variant>
#include <vector>

namespace broker {

// -- PODs ---------------------------------------------------------------------

struct add_command;
struct broker_options;
struct clear_command;
struct endpoint_info;
struct enum_value;
struct erase_command;
struct expire_command;
struct network_info;
struct node_message;
struct none;
struct peer_info;
struct publisher_id;
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
class endpoint_id;
class internal_command;
class mailbox;
class port;
class publisher;
class status;
class store;
class subnet;
class subscriber;
class topic;
class worker;

// -- templates ----------------------------------------------------------------

template <class T>
class expected;

template <class... Ts>
class cow_tuple;

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

// -- message type aliases -----------------------------------------------------

using command_message = cow_tuple<topic, internal_command>;
using data_message = cow_tuple<topic, data>;
using node_message_content = std::variant<data_message, command_message>;

} // namespace broker

// -- implementation details ---------------------------------------------------

namespace broker::detail {

class abstract_backend;

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
