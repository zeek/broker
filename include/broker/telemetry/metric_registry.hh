#pragma once

#include "broker/fwd.hh"
#include "broker/span.hh"
#include "broker/telemetry/counter.hh"
#include "broker/telemetry/gauge.hh"
#include "broker/telemetry/histogram.hh"
#include "broker/telemetry/metric_registry_impl.hh"

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string_view>
#include <vector>

namespace broker::telemetry {

/// Provides access to families.
class metric_registry {
public:
  metric_registry() = delete;

  metric_registry(metric_registry&&) noexcept;

  metric_registry(const metric_registry&) noexcept;

  metric_registry& operator=(metric_registry&&) noexcept;

  metric_registry& operator=(const metric_registry&) noexcept;

  explicit metric_registry(metric_registry_impl* impl) noexcept
    : metric_registry(impl, true) {
    // nop
  }

  ~metric_registry();

  /// Returns a metric registry handle that can be used to get access to metrics
  /// even before constructing the @ref endpoint.
  static metric_registry pre_init_instance();

  /// Merges all metrics from @p what into the endpoint @p where.
  /// @warning invalidates all other handles to the same pre-init registry!
  /// @return A handle to the registry of the endpoint.
  static metric_registry merge(metric_registry what, broker::endpoint& where);

  /// @return A handle to the registry of the endpoint.
  static metric_registry from(broker::endpoint& where);

  /// @return A counter metric family. Creates the family lazily if necessary.
  /// @param pre The prefix (namespace) this family belongs to.
  /// @param name The human-readable name of the metric, e.g., `requests`.
  /// @param labels Names for all label dimensions of the metric.
  /// @param helptext Short explanation of the metric.
  /// @param unit Unit of measurement.
  /// @param is_sum Indicates whether this metric accumulates something, where
  ///               only the total value is of interest.
  template <class T = int64_t>
  auto counter_family(std::string_view pre, std::string_view name,
                      span<const std::string_view> labels,
                      std::string_view helptext, std::string_view unit = "1",
                      bool is_sum = false) {
    if constexpr (std::is_same_v<T, int64_t>) {
      return impl_->int_counter_fam(pre, name, labels, helptext, unit, is_sum);
    } else {
      static_assert(std::is_same_v<T, double>,
                    "metrics only support int64_t and double values");
      return impl_->dbl_counter_fam(pre, name, labels, helptext, unit, is_sum);
    }
  }

  /// @copydoc counter_family
  template <class T = int64_t>
  auto counter_family(std::string_view pre, std::string_view name,
                      std::initializer_list<std::string_view> labels,
                      std::string_view helptext, std::string_view unit = "1",
                      bool is_sum = false) {
    auto lbl_span = span{labels.begin(), labels.size()};
    return counter_family<T>(pre, name, lbl_span, helptext, unit, is_sum);
  }

  /**
   * Accesses a counter instance. Creates the hosting metric family as well
   * as the counter lazily if necessary.
   * @param pre The prefix (namespace) this family belongs to.
   * @param name The human-readable name of the metric, e.g., `requests`.
   * @param labels Values for all label dimensions of the metric.
   * @param helptext Short explanation of the metric.
   * @param unit Unit of measurement.
   * @param is_sum Indicates whether this metric accumulates something, where
   *               only the total value is of interest.
   */
  template <class T = int64_t>
  counter<T>
  counter_instance(std::string_view pre, std::string_view name,
                   span<const label_view> labels, std::string_view helptext,
                   std::string_view unit = "1", bool is_sum = false) {
    return with_label_names(labels, [&, this](auto labelNames) {
      auto family = counter_family<T>(pre, name, labelNames, helptext, unit,
                                      is_sum);
      return family.getOrAdd(labels);
    });
  }

  /// @copydoc counter_instance
  template <class T = int64_t>
  counter<T> counter_instance(std::string_view pre, std::string_view name,
                              std::initializer_list<label_view> labels,
                              std::string_view helptext,
                              std::string_view unit = "1",
                              bool is_sum = false) {
    auto lbl_span = span{labels.begin(), labels.size()};
    return counter_instance(pre, name, lbl_span, helptext, unit, is_sum);
  }

  /**
   * Accesses a counter singleton, i.e., a counter that belongs to a family
   * without label dimensions (which thus only has a single member). Creates
   * the hosting metric family as well as the counter lazily if necessary.
   * @param pre The prefix (namespace) this family belongs to.
   * @param name The human-readable name of the metric, e.g., `requests`.
   * @param helptext Short explanation of the metric.
   * @param unit Unit of measurement.
   * @param is_sum Indicates whether this metric accumulates something, where
   *               only the total value is of interest.
   */
  template <class T = int64_t>
  counter<T> counter_singleton(std::string_view pre, std::string_view name,
                               std::string_view helptext,
                               std::string_view unit = "1",
                               bool is_sum = false) {
    auto labels = span<const std::string_view>{};
    auto fam = counter_family<T>(pre, name, labels, helptext, unit, is_sum);
    return fam.get_or_add({});
  }

  /**
   * @return A gauge metric family. Creates the family lazily if necessary.
   * @param pre The prefix (namespace) this family belongs to.
   * @param name The human-readable name of the metric, e.g., `requests`.
   * @param labels Names for all label dimensions of the metric.
   * @param helptext Short explanation of the metric.
   * @param unit Unit of measurement.
   * @param is_sum Indicates whether this metric accumulates something, where
   *               only the total value is of interest.
   */
  template <class T = int64_t>
  auto gauge_family(std::string_view pre, std::string_view name,
                    span<const std::string_view> labels,
                    std::string_view helptext, std::string_view unit = "1",
                    bool is_sum = false) {
    if constexpr (std::is_same_v<T, int64_t>) {
      return impl_->int_gauge_fam(pre, name, labels, helptext, unit, is_sum);
    } else {
      static_assert(std::is_same_v<T, double>,
                    "metrics only support int64_t and double values");
      return impl_->dbl_gauge_fam(pre, name, labels, helptext, unit, is_sum);
    }
  }

  /// @copydoc gauge_family
  template <class T = int64_t>
  auto gauge_family(std::string_view pre, std::string_view name,
                    std::initializer_list<std::string_view> labels,
                    std::string_view helptext, std::string_view unit = "1",
                    bool is_sum = false) {
    auto lbl_span = span{labels.begin(), labels.size()};
    return gauge_family<T>(pre, name, lbl_span, helptext, unit, is_sum);
  }

  /**
   * Accesses a gauge instance. Creates the hosting metric family as well
   * as the gauge lazily if necessary.
   * @param pre The prefix (namespace) this family belongs to.
   * @param name The human-readable name of the metric, e.g., `requests`.
   * @param labels Values for all label dimensions of the metric.
   * @param helptext Short explanation of the metric.
   * @param unit Unit of measurement.
   * @param is_sum Indicates whether this metric accumulates something, where
   *               only the total value is of interest.
   */
  template <class T = int64_t>
  gauge<T> gauge_instance(std::string_view pre, std::string_view name,
                          span<const label_view> labels,
                          std::string_view helptext,
                          std::string_view unit = "1", bool is_sum = false) {
    return with_label_names(labels, [&, this](auto labelNames) {
      auto family = gauge_family<T>(pre, name, labelNames, helptext, unit,
                                    is_sum);
      return family.getOrAdd(labels);
    });
  }

  /// @copydoc gauge_instance
  template <class T = int64_t>
  gauge<T> gauge_instance(std::string_view pre, std::string_view name,
                          std::initializer_list<label_view> labels,
                          std::string_view helptext,
                          std::string_view unit = "1", bool is_sum = false) {
    auto lbl_span = span{labels.begin(), labels.size()};
    return gauge_instance(pre, name, lbl_span, helptext, unit, is_sum);
  }

  /**
   * Accesses a gauge singleton, i.e., a gauge that belongs to a family
   * without label dimensions (which thus only has a single member). Creates
   * the hosting metric family as well as the gauge lazily if necessary.
   * @param pre The prefix (namespace) this family belongs to.
   * @param name The human-readable name of the metric, e.g., `requests`.
   * @param helptext Short explanation of the metric.
   * @param unit Unit of measurement.
   * @param is_sum Indicates whether this metric accumulates something, where
   *               only the total value is of interest.
   */
  template <class T = int64_t>
  gauge<T> gauge_singleton(std::string_view pre, std::string_view name,
                           std::string_view helptext,
                           std::string_view unit = "1", bool is_sum = false) {
    auto labels = span<const std::string_view>{};
    auto fam = gauge_family<T>(pre, name, labels, helptext, unit, is_sum);
    return fam.get_or_add({});
  }

  // Forces the compiler to use the type `span<const T>` instead of trying to
  // match paremeters to a `span`.
  template <class T>
  struct const_span_oracle {
    using type = span<const T>;
  };

  // Convenience alias to safe some typing.
  template <class T>
  using const_span = typename const_span_oracle<T>::type;

  /**
   * Returns a histogram metric family. Creates the family lazily if
   * necessary.
   * @param pre The prefix (namespace) this family belongs to. Usually the
   *            application or protocol name, e.g., `http`. The prefix `caf`
   *            as well as prefixes starting with an underscore are
   *            reserved.
   * @param name The human-readable name of the metric, e.g., `requests`.
   * @param labels Names for all label dimensions of the metric.
   * @param default_upper_bounds Upper bounds for the metric buckets.
   * @param helptext Short explanation of the metric.
   * @param unit Unit of measurement. Please use base units such as `bytes` or
   *             `seconds` (prefer lowercase). The pseudo-unit `1` identifies
   *             dimensionless counts.
   * @param is_sum Setting this to `true` indicates that this metric adds
   *               something up to a total, where only the total value is of
   *               interest. For example, the total number of HTTP requests.
   * @note The first call wins when calling this function multiple times with
   *       different bucket settings. Users may also override
   *       @p default_upper_bounds via run-time configuration.
   */
  template <class T = int64_t>
  auto histogram_family(std::string_view pre, std::string_view name,
                        span<const std::string_view> labels,
                        const_span<T> default_upper_bounds,
                        std::string_view helptext, std::string_view unit = "1",
                        bool is_sum = false) {
    if constexpr (std::is_same_v<T, int64_t>) {
      auto hdl = impl_->int_histogram_fam(pre, name, labels,
                                          default_upper_bounds, helptext, unit,
                                          is_sum);
      return int_histogram_family{hdl};
    } else {
      static_assert(std::is_same_v<T, double>,
                    "metrics only support int64_t and double values");
      auto hdl = impl_->dbl_histogram_fam(pre, name, labels,
                                          default_upper_bounds, helptext, unit,
                                          is_sum);
      return dbl_histogram_family{hdl};
    }
  }

  /// @copydoc histogram_family
  template <class T = int64_t>
  auto histogram_family(std::string_view pre, std::string_view name,
                        std::initializer_list<std::string_view> labels,
                        const_span<T> default_upper_bounds,
                        std::string_view helptext, std::string_view unit = "1",
                        bool is_sum = false) {
    auto lbl_span = span{labels.begin(), labels.size()};
    return histogram_family<T>(pre, name, lbl_span, default_upper_bounds,
                               helptext, unit, is_sum);
  }

  /**
   * Returns a histogram. Creates the family lazily if necessary.
   * @param pre The prefix (namespace) this family belongs to. Usually the
   *            application or protocol name, e.g., `http`. The prefix `caf`
   *            as well as prefixes starting with an underscore are reserved.
   * @param name The human-readable name of the metric, e.g., `requests`.
   * @param labels Names for all label dimensions of the metric.
   * @param default_upper_bounds Upper bounds for the metric buckets.
   * @param helptext Short explanation of the metric.
   * @param unit Unit of measurement. Please use base units such as `bytes` or
   *             `seconds` (prefer lowercase). The pseudo-unit `1` identifies
   *             dimensionless counts.
   * @param is_sum Setting this to `true` indicates that this metric adds
   *               something up to a total, where only the total value is of
   *               interest. For example, the total number of HTTP requests.
   * @note The first call wins when calling this function multiple times with
   *       different bucket settings. Users may also override
   *       @p default_upper_bounds via run-time configuration.
   */
  template <class T = int64_t>
  histogram<T> histogram_instance(std::string_view pre, std::string_view name,
                                  span<const label_view> labels,
                                  const_span<T> default_upper_bounds,
                                  std::string_view helptext,
                                  std::string_view unit = "1",
                                  bool is_sum = false) {
    return with_label_names(labels, [&, this](auto labelNames) {
      auto family = histogram_family<T>(pre, name, labelNames,
                                        default_upper_bounds, helptext, unit,
                                        is_sum);
      return family.getOrAdd(labels);
    });
  }

  /// @copdoc histogram_instance
  template <class T = int64_t>
  histogram<T> histogram_instance(std::string_view pre, std::string_view name,
                                  std::initializer_list<label_view> labels,
                                  const_span<T> default_upper_bounds,
                                  std::string_view helptext,
                                  std::string_view unit = "1",
                                  bool is_sum = false) {
    auto lbls = span{labels.begin(), labels.size()};
    return histogram_instance(pre, name, lbls, default_upper_bounds, helptext,
                              unit, is_sum);
  }

  /**
   * Returns a histogram metric singleton, i.e., the single instance of a
   * family without label dimensions. Creates all objects lazily if necessary,
   * but fails if the full name already belongs to a different family.
   * @param pre The prefix (namespace) this family belongs to. Usually the
   *            application or protocol name, e.g., `http`. The prefix `caf`
   *            as well as prefixes starting with an underscore are reserved.
   * @param name The human-readable name of the metric, e.g., `requests`.
   * @param default_upper_bounds Upper bounds for the metric buckets.
   * @param helptext Short explanation of the metric.
   * @param unit Unit of measurement. Please use base units such as `bytes` or
   *             `seconds` (prefer lowercase). The pseudo-unit `1` identifies
   *             dimensionless counts.
   * @param is_sum Setting this to `true` indicates that this metric adds
   *               something up to a total, where only the total value is of
   *               interest. For example, the total number of HTTP requests.
   * @note The first call wins when calling this function multiple times with
   *       different bucket settings. Users may also override
   *       @p default_upper_bounds via run-time configuration.
   */
  template <class T = int64_t>
  histogram<T> histogram_singleton(std::string_view pre, std::string_view name,
                                   const_span<T> default_upper_bounds,
                                   std::string_view helptext,
                                   std::string_view unit = "1",
                                   bool is_sum = false) {
    auto lbls = span<const std::string_view>{};
    auto fam = histogram_family<T>(pre, name, lbls, default_upper_bounds,
                                   helptext, unit, is_sum);
    return fam.get_or_add({});
  }

  [[nodiscard]] metric_registry_impl* pimpl() const noexcept {
    return impl_;
  }

private:
  metric_registry(metric_registry_impl* impl, bool add_ref) noexcept;

  template <class F>
  static void with_label_names(span<const label_view> xs, F continuation) {
    if (xs.size() <= 10) {
      std::string_view buf[10];
      for (size_t index = 0; index < xs.size(); ++index)
        buf[index] = xs[index].first;
      return continuation(span{buf, xs.size()});
    } else {
      std::vector<std::string_view> buf;
      for (auto x : xs)
        buf.emplace_back(x.first, x.second);
      return continuation(span{buf});
    }
  }

  metric_registry_impl* impl_;
};

} // namespace broker::telemetry
