#pragma once

#include <unordered_map>
#include <vector>

#include <caf/actor.hpp>
#include <caf/byte_buffer.hpp>
#include <caf/io/broker.hpp>
#include <caf/io/connection_handle.hpp>

#include "broker/detail/telemetry/collector.hh"
#include "broker/detail/telemetry/exporter.hh"
#include "broker/detail/telemetry/scraper.hh"
#include "broker/filter_type.hh"

namespace broker::detail::telemetry {

/// Makes local and remote metrics available to Prometheus via HTTP. The
/// Prometheus actor collects and exports local metrics as well as imports
/// remote metrics by subscribing to user-defined topics where other endpoints
/// periodically publish their metrics. Hence, Broker never starts an exporter
/// when starting a Prometheus actor since that would be redundant.
class prometheus_actor : public caf::io::broker {
public:
  // -- member types -----------------------------------------------------------

  using super = caf::io::broker;

  using exporter_state_type = exporter_state<super>;

  // -- constructors, destructors, and assignment operators --------------------

  explicit prometheus_actor(caf::actor_config& cfg, caf::io::doorman_ptr ptr,
                            caf::actor core);

  // -- overrides --------------------------------------------------------------

  void on_exit() override;

  const char* name() const override;

  caf::behavior make_behavior() override;

private:
  /// Caches input per open connection for parsing the HTTP header.
  std::unordered_map<caf::io::connection_handle, caf::byte_buffer> requests_;

  /// Combines various metrics into a single Prometheus output.
  collector collector_;

  /// Handle to the Broker endpoint actor.
  caf::actor core_;

  /// Filter for subscribing to metrics-related topics.
  filter_type filter_;

  /// Optional export of local metrics if the user configured a value for
  /// "broker.metrics.export.topic".
  std::unique_ptr<exporter_state_type> exporter_;
};

} // namespace broker::detail::telemetry
