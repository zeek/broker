#include "broker/telemetry/gauge.hh"

#include "broker/internal/with_native_labels.hh"

#include <caf/telemetry/gauge.hpp>
#include <caf/telemetry/metric_family.hpp>
#include <caf/telemetry/metric_family_impl.hpp>

namespace ct = caf::telemetry;

namespace broker::telemetry {

namespace {

auto& deref(dbl_gauge_hdl* hdl) {
  return *reinterpret_cast<ct::dbl_gauge*>(hdl);
}

const auto& deref(const dbl_gauge_hdl* hdl) {
  return *reinterpret_cast<const ct::dbl_gauge*>(hdl);
}

auto& deref(int_gauge_hdl* hdl) {
  return *reinterpret_cast<ct::int_gauge*>(hdl);
}

const auto& deref(const int_gauge_hdl* hdl) {
  return *reinterpret_cast<const ct::int_gauge*>(hdl);
}

auto& deref(metric_family_hdl* hdl) {
  return *reinterpret_cast<ct::metric_family*>(hdl);
}

} // namespace

void inc(dbl_gauge_hdl* hdl) noexcept {
  deref(hdl).inc();
}

void inc(dbl_gauge_hdl* hdl, double amount) noexcept {
  deref(hdl).inc(amount);
}

void dec(dbl_gauge_hdl* hdl) noexcept {
  deref(hdl).dec();
}
void dec(dbl_gauge_hdl* hdl, double amount) noexcept {
  deref(hdl).dec(amount);
}

double value(const dbl_gauge_hdl* hdl) noexcept {
  return deref(hdl).value();
}

dbl_gauge_hdl* dbl_gauge_get_or_add(metric_family_hdl* hdl,
                                    span<const label_view> labels) {
  return internal::with_native_labels(labels, [hdl](auto native_labels) {
    using derived_t = ct::metric_family_impl<ct::dbl_gauge>;
    auto res = static_cast<derived_t&>(deref(hdl)).get_or_add(native_labels);
    return reinterpret_cast<dbl_gauge_hdl*>(res);
  });
}

int64_t inc(int_gauge_hdl* hdl) noexcept {
  return ++deref(hdl);
}

void inc(int_gauge_hdl* hdl, int64_t amount) noexcept {
  deref(hdl).inc(amount);
}

int64_t dec(int_gauge_hdl* hdl) noexcept {
  return --deref(hdl);
}

void dec(int_gauge_hdl* hdl, int64_t amount) noexcept {
  deref(hdl).dec(amount);
}

int64_t value(const int_gauge_hdl* hdl) noexcept {
  return deref(hdl).value();
}

int_gauge_hdl* int_gauge_get_or_add(metric_family_hdl* hdl,
                                    span<const label_view> labels) {
  return internal::with_native_labels(labels, [hdl](auto native_labels) {
    using derived_t = ct::metric_family_impl<ct::int_gauge>;
    auto res = static_cast<derived_t&>(deref(hdl)).get_or_add(native_labels);
    return reinterpret_cast<int_gauge_hdl*>(res);
  });
}

} // namespace broker::telemetry
