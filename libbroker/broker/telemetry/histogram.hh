#pragma once

#include "broker/span.hh"
#include "broker/telemetry/fwd.hh"
#include "broker/telemetry/metric_family.hh"

#include <cstdint>

namespace broker::telemetry {

/// A handle to a metric that represents an single value that can only go up.
template <class T>
class histogram {
public:
  using handle = detail::histogram_hdl_t<T>*;

  explicit constexpr histogram(handle hdl) noexcept : hdl_(hdl) {
    // nop
  }

  histogram() = delete;

  histogram(const histogram&) noexcept = default;

  histogram& operator=(const histogram&) noexcept = default;

  /// Increments all buckets with an upper bound less than or equal to @p value
  /// by one and adds @p value to the total sum of all observed values.
  void observe(T value) noexcept {
    return telemetry::observe(hdl_, value);
  }

  /// @return The sum of all observed values.
  T sum() const noexcept {
    return telemetry::sum(hdl_);
  }

  /// @return The number of buckets, including the implicit "infinite" bucket.
  size_t num_buckets() const noexcept {
    return telemetry::num_buckets(hdl_);
  }

  /// @return The number of observations in the bucket at @p index.
  /// @pre index < NumBuckets()
  T count_at(size_t index) const noexcept {
    return telemetry::count_at(hdl_);
  }

  /// @return The upper bound of the bucket at @p index.
  /// @pre index < NumBuckets()
  T upper_bound_at(size_t index) const noexcept {
    return telemetry::upper_bound_at(hdl_, index);
  }

  /// @return Whether @c this and @p other refer to the same histogram.
  constexpr bool is_same_as(histogram other) const noexcept {
    return hdl_ == other.hdl_;
  }

private:
  handle hdl_;
};

/// @relates histogram
template <class T>
constexpr bool operator==(histogram<T> x, histogram<T> y) {
  return x.is_same_as(y);
}

/// @relates histogram
template <class T>
constexpr bool operator!=(histogram<T> x, histogram<T> y) {
  return !(x == y);
}

/// Manages a collection of @ref histogram metrics.
template <class T>
class histogram_family : public metric_family {
public:
  using super = metric_family;

  using handle = detail::histogram_fam_t<T>*;
  using const_handle = const detail::histogram_fam_t<T>*;

  explicit histogram_family(handle hdl) noexcept : super(hdl) {
    // nop
  }

  histogram_family(const histogram_family&) noexcept = default;

  histogram_family& operator=(const histogram_family&) noexcept = default;

  /// Returns the metrics handle for given labels, creating a new instance
  /// lazily if necessary.
  histogram<T> get_or_add(span<const label_view> labels) {
    if constexpr (std::is_same_v<T, double>)
      return histogram<T>{dbl_histogram_get_or_add(super::hdl_, labels)};
    else
      return histogram<T>{int_histogram_get_or_add(super::hdl_, labels)};
  }

  /// @copydoc get_or_add
  histogram<T> get_or_add(std::initializer_list<label_view> labels) {
    return get_or_add(span{labels.begin(), labels.size()});
  }

  /// @return Whether @c this and @p other refer to the same family.
  constexpr bool is_same_as(histogram_family other) const noexcept {
    return hdl_ == other.hdl_;
  }

  /// @return The number of buckets, including the implicit "infinite" bucket.
  size_t num_buckets() const noexcept {
    return telemetry::num_buckets(hdl());
  }

  /// @return The upper bound of the bucket at @p index.
  /// @pre index < NumBuckets()
  T upper_bound_at(size_t index) const noexcept {
    return telemetry::upper_bound_at(hdl(), index);
  }

private:
  const_handle hdl() const {
    if constexpr (std::is_same_v<T, double>) {
      return as_dbl_histogram_family(super::hdl_);
    } else {
      return as_int_histogram_family(super::hdl_);
    }
  }
};

/// @relates histogram_family
template <class T>
constexpr bool operator==(histogram_family<T> x, histogram_family<T> y) {
  return x.is_same_as(y);
}

/// @relates histogram_family
template <class T>
constexpr bool operator!=(histogram_family<T> x, histogram_family<T> y) {
  return !(x == y);
}

} // namespace broker::telemetry
