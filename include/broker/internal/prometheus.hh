#pragma once

#include <unordered_map>
#include <vector>

#include <caf/actor.hpp>
#include <caf/byte_buffer.hpp>
#include <caf/io/broker.hpp>
#include <caf/io/connection_handle.hpp>

#include "broker/filter_type.hh"
#include "broker/internal/metric_collector.hh"
#include "broker/internal/metric_exporter.hh"
#include "broker/internal/metric_scraper.hh"

namespace broker::internal {

/// Makes local and remote metrics available to Prometheus via HTTP. The
/// Prometheus actor collects and exports local metrics as well as imports
/// remote metrics by subscribing to user-defined topics where other endpoints
/// periodically publish their metrics. Hence, Broker never starts an exporter
/// when starting a Prometheus actor since that would be redundant.
class prometheus_actor : public caf::io::broker {
public:
  // -- member types -----------------------------------------------------------

  using super = caf::io::broker;

  using exporter_state_type = metric_exporter_state<super>;

  struct request_state {
    uint64_t async_id = 0;
    caf::byte_buffer buf;
  };

  // -- constructors, destructors, and assignment operators --------------------

  explicit prometheus_actor(caf::actor_config& cfg, caf::io::doorman_ptr ptr,
                            caf::actor core);

  // -- overrides --------------------------------------------------------------

  void on_exit() override;

  const char* name() const override;

  caf::behavior make_behavior() override;

private:
  void flush_and_close(caf::io::connection_handle hdl);

  void on_metrics_request(caf::io::connection_handle hdl);

  void on_status_request(caf::io::connection_handle hdl);

  void on_status_request_cb(caf::io::connection_handle hdl, uint64_t async_id,
                            const table& res);

  /// Caches input per open connection for parsing the HTTP header.
  std::unordered_map<caf::io::connection_handle, request_state> requests_;

  /// Combines various metrics into a single Prometheus output.
  metric_collector collector_;

  /// Handle to the Broker endpoint actor.
  caf::actor core_;

  /// Filter for subscribing to metrics-related topics.
  filter_type filter_;

  /// Optional export of local metrics if the user configured a value for
  /// "broker.metrics.export.topic".
  std::unique_ptr<exporter_state_type> exporter_;

  /// Buffer for writing JSON output.
  std::vector<char> json_buf_;
};

} // namespace broker::internal
