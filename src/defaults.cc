#include "broker/defaults.hh"

#include <chrono>
#include <limits>

using namespace std::chrono_literals;

namespace broker::defaults {

const caf::string_view recording_directory = "";

const size_t output_generator_file_cap = std::numeric_limits<size_t>::max();

const caf::timespan await_peer_timeout = 10s;

} // namespace broker::defaults

namespace broker::defaults::store {

// Run with 10 ticks per second.
const caf::timespan tick_interval = 100ms;

// Delay GET requests for up to five seconds.
const caf::timespan max_get_delay = 5s;

// Send 2 heartbeats per second.
const uint16_t heartbeat_interval = 5;

// Wait up to 200ms before sending NACK messages.
const uint16_t nack_timeout = 2;

// Disconnect channels when not hearing anything from the remote side for 10s.
const uint16_t connection_timeout = 100;

// Block up to 15s on a store object when calling await_idle.
const caf::timespan await_idle_timeout = 15s;

} // namespace broker::defaults::store

namespace broker::defaults::path_revocations {

const caf::timespan aging_interval = 1s;

const caf::timespan max_age = 5min;

} // namespace broker::defaults::path_revocations
