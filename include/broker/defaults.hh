#pragma once

#include "caf/string_view.hpp"
#include "caf/timespan.hpp"

// This header contains hard-coded default values for various Broker options.

namespace broker::defaults {

extern const caf::string_view recording_directory;

extern const size_t output_generator_file_cap;

/// Configures the default timeout of @ref endpoint::await_peer.
extern const caf::timespan await_peer_timeout;

} // namespace broker::defaults

namespace broker::defaults::subscriber {

static constexpr size_t queue_size = 64;

} // namespace broker::defaults::subscriber

namespace broker::defaults::store {

/// Configures the time interval for advancing the local Lamport time.
extern const caf::timespan tick_interval;

/// Configures how many ticks pass between sending heartbeat messages.
extern const uint16_t heartbeat_interval;

/// Configures how many ticks without any progress we wait before sending NACK
/// messages, i.e., requesting retransmits.
extern const uint16_t nack_timeout;

/// Configures how many missed heartbeats we wait before assuming the remote
/// store actore dead.
extern const uint16_t connection_timeout;

/// Configures the default timeout of @ref peer::await_idle.
extern const caf::timespan await_idle_timeout;

} // namespace broker::defaults::store

namespace broker::defaults::path_revocations {

extern const caf::timespan aging_interval;

extern const caf::timespan max_age;

} // namespace broker::defaults::path_revocations

namespace broker::defaults::metrics {

constexpr caf::timespan export_interval = std::chrono::seconds(1);

} // namespace broker::defaults::metrics
