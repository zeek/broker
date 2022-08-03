#pragma once

#include "broker/span.hh"
#include "broker/telemetry/fwd.hh"

#include <string>

namespace broker::telemetry {

/// Manages a collection (family) of metrics. All members of the family share
/// the same prefix (namespace), name, and label dimensions.
class metric_family {
public:
  metric_family() = delete;
  metric_family(const metric_family&) noexcept = default;
  metric_family& operator=(const metric_family&) noexcept = default;

  /// @return The prefix (namespace) this family belongs to. Builtin metrics
  ///         of Zeek return @c zeek. Custom metrics, e.g., created in a
  ///         script, may use a prefix that represents the application/script
  ///         or protocol (e.g. @c http) name.
  std::string_view prefix() const noexcept {
    return telemetry::prefix(hdl_);
  }

  /// @return The human-readable name of the metric, e.g., @p open-connections.
  std::string_view name() const noexcept {
    return telemetry::name(hdl_);
  }

  /// @return The names for all label dimensions.
  span<const std::string> label_names() const noexcept {
    return telemetry::label_names(hdl_);
  }

  /// @return A short explanation of the metric.
  std::string_view helptext() const noexcept {
    return telemetry::helptext(hdl_);
  }

  /// @return The unit of measurement, preferably a base unit such as @c bytes
  ///         or @c seconds. Dimensionless counts return the pseudo-unit @c 1.
  std::string_view unit() const noexcept {
    return telemetry::unit(hdl_);
  }

  /// @return Whether metrics of this family accumulate values, where only the
  ///         total value is of interest. For example, the total number of
  ///         HTTP requests.
  bool is_sum() const noexcept {
    return telemetry::is_sum(hdl_);
  }

protected:
  /// @pre `hdl != nullptr`
  template <class Derived>
  constexpr explicit metric_family(Derived* hdl) noexcept : hdl_(upcast(hdl)) {
    // nop
  }

  metric_family_hdl* hdl_;
};

} // namespace broker::telemetry
