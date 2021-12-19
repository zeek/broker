#pragma once

#include "broker/data.hh"
#include "broker/topic.hh"

#include <caf/telemetry/metric_registry.hpp>

namespace broker::internal {

/// Scrapes local CAF metrics and encodes them into `data` objects (for
/// decoding, see `metric_view`). The `data`-encoded metrics then may get
/// published to a Broker topic by an exporter or Prometheus actor.
class metric_scraper {
public:
  // -- constructors, destructors, and assignment operators --------------------

  metric_scraper(std::string id);

  metric_scraper(std::vector<std::string> selected_prefixes, std::string id);

  // -- properties -------------------------------------------------------------

  [[nodiscard]] const auto& selected_prefixes() const noexcept {
    return selected_prefixes_;
  }

  template <class T>
  void selected_prefixes(T&& new_prefixes) {
    selected_prefixes_ = std::forward<T>(new_prefixes);
  }

  [[nodiscard]] auto last_scrape() const noexcept {
    return last_scrape_;
  }

  [[nodiscard]] const auto& id() const noexcept {
    return id_;
  }

  void id(std::string new_id);

  [[nodiscard]] const auto& rows() const noexcept {
    return rows_;
  }

  /// Checks whether `selected_prefixes` is empty (an empty filter means *select
  /// all*) or `family->prefix()` is in `selected_prefixes`.
  bool selected(const caf::telemetry::metric_family* family);

  // -- callbacks for the metric registry --------------------------------------

  /// Encodes selected metrics from the CAF metrics registry to a table-like
  /// data structure and stores the result in `rows`.
  void scrape(caf::telemetry::metric_registry& registry);

  void operator()(const caf::telemetry::metric_family* family,
                  const caf::telemetry::metric* instance,
                  const caf::telemetry::dbl_counter* counter);

  void operator()(const caf::telemetry::metric_family* family,
                  const caf::telemetry::metric* instance,
                  const caf::telemetry::int_counter* counter);

  void operator()(const caf::telemetry::metric_family* family,
                  const caf::telemetry::metric* instance,
                  const caf::telemetry::dbl_gauge* gauge);

  void operator()(const caf::telemetry::metric_family* family,
                  const caf::telemetry::metric* instance,
                  const caf::telemetry::int_gauge* gauge);

  void operator()(const caf::telemetry::metric_family* family,
                  const caf::telemetry::metric* instance,
                  const caf::telemetry::dbl_histogram* histogram);

  void operator()(const caf::telemetry::metric_family* family,
                  const caf::telemetry::metric* instance,
                  const caf::telemetry::int_histogram* histogram);

  template <class T>
  void operator()(const caf::telemetry::metric_family*,
                  const caf::telemetry::metric*, const T*) {
    // nop
  }

private:
  // -- private utility --------------------------------------------------------

  /// Encodes a single metric as a row in our output vector.
  template <class T>
  void add_row(const caf::telemetry::metric_family* family, std::string type,
               table labels, T value);

  // -- member variables -------------------------------------------------------

  /// Configures which metrics the scraper includes.
  std::vector<std::string> selected_prefixes_;

  /// Stores the system time of the last call to `scrape()`.
  timestamp last_scrape_;

  /// Configures the ID of this scraper to allow subscribers to disambiguate
  /// remote metrics. By default, the scraper uses the target suffix as ID. For
  /// example, constructing the scraper with target topic
  /// `/zeek/metrics/worker-1` would set this field to "worker-1".
  std::string id_;

  /// Contains the result for the last scraping run as data rows. The first row
  /// is reserved for meta data (scraper ID plus timestamp).
  vector rows_;
};

} // namespace broker::internal
