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

// Run with 20 ticks per second.
const caf::timespan tick_interval = 50ms;

// Send 5 heartbeats per second.
const uint16_t heartbeat_interval = 4;

// Wait up to 100ms before sending NACK messages.
const uint16_t nack_timeout = 2;

// Disconnect channels when not hearing anything from the remote side for 1s.
const uint16_t connection_timeout = 5;

const caf::timespan await_idle_timeout = 5s;

} // namespace broker::defaults::store

namespace broker::defaults::path_revocations {

const caf::timespan aging_interval = 1s;

const caf::timespan max_age = 5min;

} // namespace broker::defaults::path_revocations
