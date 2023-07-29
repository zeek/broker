#pragma once

#include "broker/data.hh"
#include "broker/filter_type.hh"
#include "broker/internal/metric_exporter.hh"

#include <caf/actor.hpp>
#include <caf/byte_buffer.hpp>
#include <caf/io/broker.hpp>
#include <caf/io/connection_handle.hpp>
#include <caf/telemetry/collector/prometheus.hpp>
#include <caf/telemetry/importer/process.hpp>

#include <unordered_map>
#include <vector>

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
  /// When assembling the response, we need to "merge" all inputs into a single
  /// group per metric name.
  struct metric_group {
    /// The type of the metric, e.g., counter, gauge, or histogram.
    std::string type;

    /// The help string for the metric.
    std::string help;

    /// The Prometheus-encoded lines for the metric.
    std::vector<char> lines;
  };

  /// Maps metric names to metric groups.
  using metric_groups = std::unordered_map<std::string, metric_group>;

  // Adds lines from the range `(pos, end)` to `group` until either reaching the
  // end or finding the start of a new metric.
  caf::string_view::iterator merge_metrics(const std::string& endpoint_name,
                                           caf::string_view metric_name,
                                           std::vector<char>& lines,
                                           caf::string_view::iterator pos,
                                           caf::string_view::iterator end);

  // Splits `prom_txt` into lines and adds them to `metric_groups`.
  void merge_metrics(const std::string& endpoint_name,
                     caf::string_view prom_txt);

  void flush_and_close(caf::io::connection_handle hdl);

  void on_metrics_request(caf::io::connection_handle hdl);

  void on_status_request(caf::io::connection_handle hdl);

  void on_status_request_cb(caf::io::connection_handle hdl, uint64_t async_id,
                            const table& res);

  /// Caches input per open connection for parsing the HTTP header.
  std::unordered_map<caf::io::connection_handle, request_state> requests_;

  /// Caches metrics from remote Broker instances. The key is the remote
  /// endpoint's name (broker.metrics.endpoint-name) and the value is the
  /// rendered metrics in Prometheus text format.
  std::map<std::string, std::string> remote_metrics_;

  /// Handle to the Broker endpoint actor.
  caf::actor core_;

  /// Filter for subscribing to metrics-related topics.
  filter_type filter_;

  /// Buffer for rendering Prometheus or JSON output.
  std::vector<char> buf_;

  /// Caches metrics while we merge them into groups.
  metric_groups metric_groups_;

  /// The exporter for remote metrics.
  metric_exporter exporter_;
};

} // namespace broker::internal
