#pragma once

#include <set>
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

  using label_list = std::vector<caf::telemetry::label>;

  using label_view_list = std::vector<caf::telemetry::label_view>;

  using string_span = caf::span<const std::string_view>;

  class remote_metric : public caf::telemetry::metric {
  public:
    using super = caf::telemetry::metric;

    remote_metric(label_list labels,
                  const caf::telemetry::metric_family* parent);

    ~remote_metric() override;

    virtual void update(metric_view mv) = 0;

    virtual void append_to(caf::telemetry::collector::prometheus&) = 0;

  protected:
    const caf::telemetry::metric_family* parent_;
  };

  using family_ptr = std::unique_ptr<caf::telemetry::metric_family>;

  using instance_ptr = std::unique_ptr<remote_metric>;

  /// Predicate for checking whether a list of labels is less than another list
  /// of labels. Automatically un-boxes `instance_ptr`.
  struct labels_less {
    using is_transparent = std::true_type;

    /// Compares a two individual label elements.
    template <class T1, class T2>
    auto cmp_element(const T1& lhs, const T2& rhs) const noexcept {
      auto cmp1 = lhs.name().compare(rhs.name());
      return cmp1 != 0 ? cmp1 : lhs.value().compare(rhs.value());
    }

    template <class T1, class T2>
    bool operator()(const T1& lhs, const T2& rhs) const noexcept {
      using remote_metric = std::unique_ptr<metric_collector::remote_metric>;
      if constexpr (std::is_same_v<T1, remote_metric>) {
        return (*this)(lhs->labels(), rhs);
      } else if constexpr (std::is_same_v<T2, remote_metric>) {
        return (*this)(lhs, rhs->labels());
      } else {
        if (lhs.size() != rhs.size())
          return lhs.size() < rhs.size();
        // Empty lists are equal.
        if (lhs.empty())
          return false;
        // Compare the first n - 1 labels.
        size_t index = 0;
        for (; index < lhs.size() - 1; ++index) {
          auto res = cmp_element(lhs[index], rhs[index]);
          // If a label was less than the corresponding label in the other list,
          // we can return true immediately.
          if (res < 0)
            return true;
          // If a label was greater than the corresponding label in the other
          // list, we can return false immediately.
          if (res > 0)
            return false;
          // Otherwise, the labels are equal and we continue with the next
          // label.
        }
        // Compare the last label. Must be less.
        return lhs[index] < rhs[index];
      }
    }
  };

  /// Predicate for checking whether two lists of labels are equal.
  /// Automatically un-boxes `instance_ptr`.
  struct labels_equal {
    using is_transparent = std::true_type;

    template <class T1, class T2>
    bool operator()(const T1& lhs, const T2& rhs) const {
      using remote_metric = std::unique_ptr<metric_collector::remote_metric>;
      if constexpr (std::is_same_v<T1, remote_metric>) {
        return (*this)(lhs->labels(), rhs);
      } else if constexpr (std::is_same_v<T2, remote_metric>) {
        return (*this)(lhs, rhs->labels());
      } else {
        if (lhs.size() != rhs.size())
          return false;
        for (size_t index = 0; index < lhs.size(); ++index) {
          if (lhs[index] != rhs[index])
            return false;
        }
        return true;
      }
    }
  };

  // --- constructors and destructors ------------------------------------------

  metric_collector();

  ~metric_collector();

  // -- data management --------------------------------------------------------

  size_t insert_or_update(const data& content);

  size_t insert_or_update(const vector& vec);

  size_t insert_or_update(const std::string& endpoint_name, timestamp ts,
                          caf::span<const data> rows);

  /// Returns the recorded metrics in the Prometheus text format.
  [[nodiscard]] std::string_view prometheus_text();

  void clear();

private:
  // -- private member types ---------------------------------------------------

  struct metric_scope {
    /// The metric family.
    family_ptr family;
    /// The instances of the metric family, sorted by label values.
    std::set<instance_ptr, labels_less> instances;
  };

  using name_map = std::unordered_map<std::string, metric_scope>;

  using prefix_map = std::unordered_map<std::string, name_map>;

  // -- time management --------------------------------------------------------

  /// Tries to advance the last-seen-time for given endpoint.
  bool advance_time(const std::string& endpoint_name, timestamp current_time);

  // -- lookups ----------------------------------------------------------------

  /// Extracts the names for all label dimensions from `mv`.
  string_span label_names_for(metric_view mv);

  /// Extracts the label dimensions from `mv` and stores them in `result`.
  void labels_for(const std::string& endpoint_name, metric_view mv,
                  label_view_list& result);

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

  /// Caches the string "endpoint" as a broker::data instance. Having this as a
  /// member avoids constructing this object each time in `labels_for`.
  data ep_key_ = data{"endpoint"};
};

} // namespace broker::internal
