#ifndef BROKER_FWD_HH
#define BROKER_FWD_HH

#include <cstdint>

namespace broker {

class configuration;

class endpoint;
struct endpoint_info;
struct network_info;
struct peer_info;

class publisher;
class subscriber;
class topic;

class data;
class status;

class store;

class internal_command;

struct add_command;
struct erase_command;
struct put_command;
struct put_unique_command;
struct set_command;
struct snapshot_command;
struct snapshot_sync_command;
struct subtract_command;

/// A monotonic identifier to represent a specific lookup request.
using request_id = uint64_t;

// Arithmetic data types
using boolean = bool;
using count = uint64_t;
using integer = int64_t;
using real = double;

namespace bro {

class Event;
class RelayEvent;
class HandleAndRelayEvent;
class LogCreate;
class LogWrite;
class IdentifierUpdate;

} // namespace bro

namespace detail {

class flare_actor;
class mailbox;

} // namespace detail

} // namespace broker

#endif // BROKER_FWD_HH
