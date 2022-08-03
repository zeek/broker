#pragma once

#include "broker/span.hh"
#include "broker/telemetry/fwd.hh"
#include "broker/telemetry/metric_family.hh"

#include <cstdint>
#include <type_traits>

namespace broker::telemetry {

/// Increments the value of @p hdl by @p amount.
void inc(dbl_counter_hdl*, double amount) noexcept;

/// Returns the value of @p hdl.
double value(dbl_counter_hdl*) noexcept;

/// @pre @p hdl must be a dbl_counter family handle.
dbl_counter_hdl* dbl_counter_get_or_add(metric_family_hdl,
                                        span<telemetry::label_view> labels);

/// Increments the value of @p hdl by one.
int64_t inc(int_counter_hdl*) noexcept;

/// Increments the value of @p hdl by @p amount.
void inc(int_counter_hdl*, int64_t amount) noexcept;

/// Returns the value of @p hdl.
int64_t value(int_counter_hdl*) noexcept;

/// @pre @p hdl must be an int_counter family handle.
int_counter_hdl* int_counter_get_or_add(metric_family_hdl*,
                                        span<telemetry::label_view> labels);

/// A handle to a metric that represents an single value that can only go up.
template <class T>
class counter {
public:
  using handle = detail::counter_hdl_t<T>*;

  explicit constexpr counter(handle hdl) noexcept : hdl_(hdl) {
    // nop
  }

  counter() = delete;

  counter(const counter&) noexcept = default;

  counter& operator=(const counter&) noexcept = default;

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

  /// @return The current value.
  T value() const noexcept {
    return telemetry::value(hdl_);
  }

  /// @return Whether @c this and @p other refer to the same counter.
  constexpr bool is_same_as(counter other) const noexcept {
    return hdl_ == other.hdl_;
  }

private:
  handle hdl_;
};

/// @relates counter
template <class T>
constexpr bool operator==(counter<T> x, counter<T> y) {
  return x.is_same_as(y);
}

/// @relates counter
template <class T>
constexpr bool operator!=(counter<T> x, counter<T> y) {
  return !(x == y);
}

/// Manages a collection of @ref counter metrics.
template <class T>
class counter_family : public metric_family {
public:
  using super = metric_family;

  using handle = detail::counter_fam_t<T>*;

  explicit counter_family(handle hdl) noexcept : super(hdl) {
    // nop
  }

  counter_family(const counter_family&) noexcept = default;

  counter_family& operator=(const counter_family&) noexcept = default;

  /// Returns the metrics handle for given labels, creating a new instance
  /// lazily if necessary.
  counter<T> get_or_add(span<const label_view> labels) {
    if constexpr (std::is_same_v<T, double>)
      return counter<T>{dbl_counter_get_or_add(super::hdl_, labels)};
    else
      return counter<T>{int_counter_get_or_add(super::hdl_, labels)};
  }

  /// @copydoc get_or_add
  counter<T> get_or_add(std::initializer_list<label_view> labels) {
    return get_or_add(span{labels.begin(), labels.size()});
  }

  /// @return Whether @c this and @p other refer to the same family.
  constexpr bool is_same_as(counter_family other) const noexcept {
    return hdl_ == other.hdl_;
  }
};

/// @relates counter_family
template <class T>
constexpr bool operator==(counter_family<T> x, counter_family<T> y) {
  return x.is_same_as(y);
}

/// @relates counter_family
template <class T>
constexpr bool operator!=(counter_family<T> x, counter_family<T> y) {
  return !(x == y);
}

} // namespace broker::telemetry
