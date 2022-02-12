#include "broker/telemetry/histogram.hh"

#include "broker/detail/assert.hh"
#include "broker/internal/with_native_labels.hh"

#include <caf/telemetry/histogram.hpp>
#include <caf/telemetry/metric_family.hpp>
#include <caf/telemetry/metric_family_impl.hpp>

namespace ct = caf::telemetry;

namespace broker::telemetry {

namespace {

auto& deref(dbl_histogram_hdl* hdl) {
  return *reinterpret_cast<ct::dbl_histogram*>(hdl);
}

auto& deref(int_histogram_hdl* hdl) {
  return *reinterpret_cast<ct::int_histogram*>(hdl);
}

auto& deref(metric_family_hdl* hdl) {
  return *reinterpret_cast<ct::metric_family*>(hdl);
}

} // namespace

void observe(dbl_histogram_hdl* hdl, double value) noexcept {
  deref(hdl).observe(value);
}

double sum(dbl_histogram_hdl* hdl) noexcept {
  return deref(hdl).sum();
}

size_t num_buckets(dbl_histogram_hdl* hdl) noexcept {
  return deref(hdl).buckets().size();
}

double count_at(dbl_histogram_hdl* hdl, size_t index) noexcept {
  auto xs = deref(hdl).buckets();
  BROKER_ASSERT(index < xs.size());
  return xs[index].count.value();
}

double upper_bound_at(dbl_histogram_hdl* hdl, size_t index) noexcept {
	auto xs = deref(hdl).buckets();
	BROKER_ASSERT(index < xs.size());
	return xs[index].upper_bound;
}

dbl_histogram_hdl* dbl_histogram_get_or_add(metric_family_hdl* hdl,
                                            span<const label_view> xs) {
  return internal::with_native_labels(xs, [hdl](auto native_labels) {
    using derived_t = ct::metric_family_impl<ct::dbl_histogram>;
    auto res = static_cast<derived_t&>(deref(hdl)).get_or_add(native_labels);
    return reinterpret_cast<dbl_histogram_hdl*>(res);
  });
}

void observe(int_histogram_hdl* hdl, int64_t value) noexcept {
  deref(hdl).observe(value);
}

int64_t sum(int_histogram_hdl* hdl) noexcept {
  return deref(hdl).sum();
}

size_t num_buckets(int_histogram_hdl* hdl) noexcept {
  return deref(hdl).buckets().size();
}

int64_t count_at(int_histogram_hdl* hdl, size_t index) noexcept {
  auto xs = deref(hdl).buckets();
  BROKER_ASSERT(index < xs.size());
  return xs[index].count.value();
}

int64_t upper_bound_at(int_histogram_hdl* hdl, size_t index) noexcept {
	auto xs = deref(hdl).buckets();
	BROKER_ASSERT(index < xs.size());
	return xs[index].upper_bound;
}

int_histogram_hdl* int_histogram_get_or_add(metric_family_hdl* hdl,
                                            span<const label_view> xs) {
  return internal::with_native_labels(xs, [hdl](auto native_labels) {
    using derived_t = ct::metric_family_impl<ct::int_histogram>;
    auto res = static_cast<derived_t&>(deref(hdl)).get_or_add(native_labels);
    return reinterpret_cast<int_histogram_hdl*>(res);
  });
}

} // namespace broker::telemetry
