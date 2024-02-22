#pragma once

#include "broker/time.hh"

#include <chrono>
#include <limits>
#include <string_view>

// This header contains hard-coded default values for various Broker options.

namespace broker::defaults {

constexpr uint16_t ttl = 16;

constexpr std::string_view recording_directory = "";

constexpr size_t output_generator_file_cap = std::numeric_limits<size_t>::max();

/// Configures the default timeout of @ref endpoint::await_peer.
constexpr timespan await_peer_timeout = std::chrono::seconds{10};

/// Configures the default timeout for unpeering from another node.
constexpr timespan unpeer_timeout = std::chrono::seconds{3};

} // namespace broker::defaults

namespace broker::defaults::subscriber {

static constexpr size_t queue_size = 64;

} // namespace broker::defaults::subscriber

namespace broker::defaults::store {

constexpr timespan tick_interval = std::chrono::milliseconds{100};

/// Configures the maximum delay for GET requests while waiting for the master.
constexpr timespan max_get_delay = std::chrono::seconds{5};

/// Configures how many ticks pass between sending heartbeat messages.
constexpr uint16_t heartbeat_interval = 5; // 2 per second

/// Configures how many ticks without any progress we wait before sending NACK
/// messages, i.e., requesting retransmits.
constexpr uint16_t nack_timeout = 2; // 200ms

/// Configures how many missed heartbeats we wait before assuming the remote
/// store actore dead.
constexpr uint16_t connection_timeout = 100; // 10s

/// Configures the default timeout of @ref peer::await_idle.
constexpr timespan await_idle_timeout = std::chrono::seconds{15};

} // namespace broker::defaults::store

namespace broker::defaults::path_revocations {

constexpr timespan aging_interval = std::chrono::seconds{1};

constexpr timespan max_age = std::chrono::seconds{5};

} // namespace broker::defaults::path_revocations

namespace broker::defaults::metrics {

constexpr timespan export_interval = std::chrono::seconds{1};

} // namespace broker::defaults::metrics
