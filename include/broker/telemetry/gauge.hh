#pragma once

#include "broker/span.hh"
#include "broker/telemetry/fwd.hh"
#include "broker/telemetry/metric_family.hh"

#include <cstdint>
#include <type_traits>

namespace broker::telemetry {

/// Increments the value of @p hdl by @p amount.
void inc(telemetry::dbl_gauge_hdl* hdl, double amount) noexcept;

/// Decrements the value of @p hdl by @p amount.
void dec(dbl_gauge_hdl* hdl, double amount) noexcept;

/// Returns the value of @p hdl.
double value(dbl_gauge_hdl* hdl) noexcept;

/// @pre @p hdl must be a dbl_gauge family handle.
dbl_gauge_hdl* dbl_gauge_get_or_add(metric_family_hdl* hdl,
                                    span<telemetry::label_view> labels);

/// Increments the value of @p hdl by one.
int64_t inc(int_gauge_hdl* hdl) noexcept;

/// Increments the value of @p hdl by @p amount.
void inc(int_gauge_hdl* hdl, int64_t amount) noexcept;

/// Decrements the value of @p hdl by one.
int64_t dec(int_gauge_hdl* hdl) noexcept;

/// Decrements the value of @p hdl by @p amount.
void dec(int_gauge_hdl* hdl, int64_t amount) noexcept;

/// Returns the value of @p hdl.
int64_t value(int_gauge_hdl* hdl) noexcept;

/// @pre @p hdl must be an int_gauge family handle.
int_gauge_hdl* int_gauge_get_or_add(metric_family_hdl* hdl,
                                    span<telemetry::label_view> labels);

/// A handle to a metric that represents an single value. Wraps an opaque gauge
/// handle to provide a class-based interface.
template <class T>
class gauge {
public:
  using handle = detail::gauge_hdl_t<T>*;

  explicit constexpr gauge(handle hdl) noexcept : hdl_(hdl) {
    // nop
  }

  gauge() = delete;

  gauge(const gauge&) noexcept = default;

  gauge& operator=(const gauge&) noexcept = default;

  /// Increments the value by 1.
  void inc() noexcept {
    telemetry::inc(hdl_);
  }

  /// Increments the value by @p amount.
  /// @pre `amount >= 0`
  void inc(T amount) noexcept {
    telemetry::inc(hdl_, amount);
  }

  /// Increments the value by 1.
  /// @return The new value.
  template <class V = T>
  std::enable_if_t<std::is_same_v<V, int64_t>, V> operator++() noexcept {
    return telemetry::inc(hdl_);
  }

  /// Decrements the value by 1.
  void dec() noexcept;

  /// Decrements the value by @p amount.
  void dec(int64_t amount) noexcept;

  /// Decrements the value by 1.
  /// @return The new value.
  template <class V = T>
  std::enable_if_t<std::is_same_v<V, int64_t>, V> operator--() noexcept {
    return telemetry::dec(hdl_);
  }

  /// @return The current value.
  T value() const noexcept {
    return telemetry::value(hdl_);
  }

  /// @return Whether @c this and @p other refer to the same gauge.
  constexpr bool is_same_as(gauge other) const noexcept {
    return hdl_ == other.hdl_;
  }

private:
  handle hdl_;
};

/// @relates gauge
template <class T>
constexpr bool operator==(gauge<T> x, gauge<T> y) {
  return x.is_same_as(y);
}

/// @relates gauge
template <class T>
constexpr bool operator!=(gauge<T> x, gauge<T> y) {
  return !(x == y);
}

/// Manages a collection of @ref gauge metrics.
template <class T>
class gauge_family : public metric_family {
public:
  using super = metric_family;

  using handle = detail::gauge_fam_t<T>*;

  explicit gauge_family(handle hdl) noexcept : super(hdl) {
    // nop
  }

  gauge_family(const gauge_family&) noexcept = default;

  gauge_family& operator=(const gauge_family&) noexcept = default;

  /// Returns the metrics handle for given labels, creating a new instance
  /// lazily if necessary.
  gauge<T> get_or_add(span<const label_view> labels) {
    if constexpr (std::is_same_v<T, double>)
      return gauge<T>{dbl_gauge_get_or_add(super::hdl_, labels)};
    else
      return gauge<T>{int_gauge_get_or_add(super::hdl_, labels)};
  }

  /// @copydoc get_or_add
  gauge<T> get_or_add(std::initializer_list<label_view> labels) {
    return get_or_add(span{labels.begin(), labels.size()});
  }

  /// @return Whether @c this and @p other refer to the same family.
  constexpr bool is_same_as(gauge_family other) const noexcept {
    return hdl_ == other.hdl_;
  }
};

/// @relates gauge_family
template <class T>
constexpr bool operator==(gauge_family<T> x, gauge_family<T> y) {
  return x.is_same_as(y);
}

/// @relates gauge_family
template <class T>
constexpr bool operator!=(gauge_family<T> x, gauge_family<T> y) {
  return !(x == y);
}

} // namespace broker::telemetry
