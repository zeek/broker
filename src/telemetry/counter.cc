#include "broker/telemetry/counter.hh"

#include "broker/internal/with_native_labels.hh"

#include <caf/telemetry/counter.hpp>
#include <caf/telemetry/metric_family.hpp>
#include <caf/telemetry/metric_family_impl.hpp>

namespace ct = caf::telemetry;

namespace broker::telemetry {

namespace {

auto& deref(dbl_counter_hdl* hdl) {
  return *reinterpret_cast<ct::dbl_counter*>(hdl);
}

const auto& deref(const dbl_counter_hdl* hdl) {
  return *reinterpret_cast<const ct::dbl_counter*>(hdl);
}

auto& deref(int_counter_hdl* hdl) {
  return *reinterpret_cast<ct::int_counter*>(hdl);
}

const auto& deref(const int_counter_hdl* hdl) {
  return *reinterpret_cast<const ct::int_counter*>(hdl);
}

auto& deref(metric_family_hdl* hdl) {
  return *reinterpret_cast<ct::metric_family*>(hdl);
}

} // namespace

void inc(dbl_counter_hdl* hdl) noexcept {
  deref(hdl).inc();
}

void inc(dbl_counter_hdl* hdl, double amount) noexcept {
  deref(hdl).inc(amount);
}

double value(const dbl_counter_hdl* hdl) noexcept {
  return deref(hdl).value();
}

dbl_counter_hdl* dbl_counter_get_or_add(metric_family_hdl* hdl,
                                        span<const label_view> labels) {
  return internal::with_native_labels(labels, [hdl](auto native_labels) {
    using derived_t = ct::metric_family_impl<ct::dbl_counter>;
    auto res = static_cast<derived_t&>(deref(hdl)).get_or_add(native_labels);
    return reinterpret_cast<dbl_counter_hdl*>(res);
  });
}

int64_t inc(int_counter_hdl* hdl) noexcept {
  return ++deref(hdl);
}

void inc(int_counter_hdl* hdl, int64_t amount) noexcept {
  deref(hdl).inc(amount);
}

int64_t value(const int_counter_hdl* hdl) noexcept {
  return deref(hdl).value();
}

int_counter_hdl* int_counter_get_or_add(metric_family_hdl* hdl,
                                        span<const label_view> labels) {
  return internal::with_native_labels(labels, [hdl](auto native_labels) {
    using derived_t = ct::metric_family_impl<ct::int_counter>;
    auto res = static_cast<derived_t&>(deref(hdl)).get_or_add(native_labels);
    return reinterpret_cast<int_counter_hdl*>(res);
  });
}

} // namespace broker::telemetry
