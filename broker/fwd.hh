#ifndef BROKER_FWD_HH
#define BROKER_FWD_HH

#include <cstdint>

namespace broker {

class blocking_endpoint;
class context;
class data;
class endpoint;
class message;
class nonblocking_endpoint;
class publisher;
class status;
class store;
class subscriber;
class topic;
class internal_command;

struct network_info;

using endpoint_id = uint64_t;

/// A monotonic identifier to represent a specific lookup request.
using request_id = uint64_t;

// Arithmetic data types
using boolean = bool;
using count = uint64_t;
using integer = int64_t;
using real = double;

namespace detail {

class flare_actor;
class mailbox;

struct put_command;
struct erase_command;
struct add_command;
struct subtract_command;
struct snapshot_command;

} // namespace detail

} // namespace broker

#endif // BROKER_FWD_HH
