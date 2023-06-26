#pragma once

#include <string_view>
#include <utility>
#include <vector>

#include <caf/span.hpp>
#include <caf/telemetry/collector/prometheus.hpp>
#include <caf/telemetry/label_view.hpp>
#include <caf/telemetry/metric.hpp>
#include <caf/telemetry/metric_family.hpp>
#include <caf/telemetry/metric_type.hpp>

#include "broker/data.hh"
#include "broker/detail/assert.hh"
#include "broker/fwd.hh"
#include "broker/internal/metric_view.hh"
#include "broker/time.hh"
#include "broker/topic.hh"

namespace broker::internal {

/// Subscribes to a topic for receiving remote metrics and makes them accessible
/// in the Prometheus text format.
class metric_collector {
public:
  // -- member types -----------------------------------------------------------

  template <class T>
  using mono_pair = std::pair<T, T>;

  using label_span = caf::span<const caf::telemetry::label_view>;

  using string_span = caf::span<const std::string_view>;

  class remote_metric : public caf::telemetry::metric {
  public:
    using super = caf::telemetry::metric;

    remote_metric(std::vector<caf::telemetry::label> labels,
                  const caf::telemetry::metric_family* parent);

    ~remote_metric() override;

    virtual void update(metric_view mv) = 0;

    virtual void append_to(caf::telemetry::collector::prometheus&) = 0;

  protected:
    const caf::telemetry::metric_family* parent_;
  };

  // --- constructors and destructors ------------------------------------------

  metric_collector();

  ~metric_collector();

  // -- data management --------------------------------------------------------

  size_t insert_or_update(const data& content);

  size_t insert_or_update(const data_view& content);

  size_t insert_or_update(const vector& vec);

  size_t insert_or_update(const std::string& endpoint_name, timestamp ts,
                          caf::span<const data> rows);

  /// Returns the recorded metrics in the Prometheus text format.
  [[nodiscard]] std::string_view prometheus_text();

  void clear();

private:
  // -- private member types ---------------------------------------------------

  using family_ptr = std::unique_ptr<caf::telemetry::metric_family>;

  using instance_ptr = std::unique_ptr<remote_metric>;

  struct metric_scope {
    family_ptr family;
    std::vector<instance_ptr> instances;
  };

  using name_map = std::unordered_map<std::string, metric_scope>;

  using prefix_map = std::unordered_map<std::string, name_map>;

  // -- time management --------------------------------------------------------

  /// Tries to advance the last-seen-time for given endpoint.
  bool advance_time(const std::string& endpoint_name, timestamp current_time);

  // -- lookups ----------------------------------------------------------------

  /// Extracts the names for all label dimensions from `mv`.
  string_span label_names_for(metric_view mv);

  /// Extracts the label dimensions from `mv`.
  label_span labels_for(const std::string& endpoint_name, metric_view mv);

  /// Retrieves or lazily creates a metric object for `mv`.
  remote_metric* instance(const std::string& endpoint_name, metric_view mv);

  /// Caches labels (key/value pairs) for instance lookups.
  std::vector<caf::telemetry::label_view> labels_;

  /// Caches label names (dimensions) for instance lookups.
  std::vector<std::string_view> label_names_;

  /// Top-level lookup structure to navigate from prefixes to name maps.
  prefix_map prefixes_;

  /// Stores last-seen-times by endpoints.
  std::unordered_map<std::string, timestamp> last_seen_;

  /// Generates Prometheus-formatted text.
  caf::telemetry::collector::prometheus generator_;
};

} // namespace broker::internal
