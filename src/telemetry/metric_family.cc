#include "broker/telemetry/metric_family.hh"

#include <caf/telemetry/counter.hpp>
#include <caf/telemetry/gauge.hpp>
#include <caf/telemetry/histogram.hpp>
#include <caf/telemetry/metric_family.hpp>
#include <caf/telemetry/metric_family_impl.hpp>

namespace ct = caf::telemetry;

namespace broker::telemetry {

namespace {

auto& deref(const metric_family_hdl* hdl) {
  return *reinterpret_cast<const ct::metric_family*>(hdl);
}

metric_family_hdl* opaque(ct::metric_family* ptr) {
  return reinterpret_cast<metric_family_hdl*>(ptr);
}

const metric_family_hdl* opaque(const ct::metric_family* ptr) {
  return reinterpret_cast<const metric_family_hdl*>(ptr);
}

} // namespace

metric_family_hdl* upcast(dbl_counter_family_hdl* ptr) {
  using native_t = ct::metric_family_impl<ct::dbl_counter>;
  return opaque(reinterpret_cast<native_t*>(ptr));
}

const metric_family_hdl* upcast(const dbl_counter_family_hdl* ptr) {
  using native_t = ct::metric_family_impl<ct::dbl_counter>;
  return opaque(reinterpret_cast<const native_t*>(ptr));
}

metric_family_hdl* upcast(dbl_gauge_family_hdl* ptr) {
  using native_t = ct::metric_family_impl<ct::dbl_gauge>;
  return opaque(reinterpret_cast<native_t*>(ptr));
}

const metric_family_hdl* upcast(const dbl_gauge_family_hdl* ptr) {
  using native_t = ct::metric_family_impl<ct::dbl_gauge>;
  return opaque(reinterpret_cast<const native_t*>(ptr));
}

metric_family_hdl* upcast(dbl_histogram_family_hdl* ptr) {
  using native_t = ct::metric_family_impl<ct::dbl_histogram>;
  return opaque(reinterpret_cast<native_t*>(ptr));
}

const metric_family_hdl* upcast(const dbl_histogram_family_hdl* ptr) {
  using native_t = ct::metric_family_impl<ct::dbl_histogram>;
  return opaque(reinterpret_cast<const native_t*>(ptr));
}

metric_family_hdl* upcast(int_counter_family_hdl* ptr) {
  using native_t = ct::metric_family_impl<ct::int_counter>;
  return opaque(reinterpret_cast<native_t*>(ptr));
}

const metric_family_hdl* upcast(const int_counter_family_hdl* ptr) {
  using native_t = ct::metric_family_impl<ct::int_counter>;
  return opaque(reinterpret_cast<const native_t*>(ptr));
}

metric_family_hdl* upcast(int_gauge_family_hdl* ptr) {
  using native_t = ct::metric_family_impl<ct::int_gauge>;
  return opaque(reinterpret_cast<native_t*>(ptr));
}

const metric_family_hdl* upcast(const int_gauge_family_hdl* ptr) {
  using native_t = ct::metric_family_impl<ct::int_gauge>;
  return opaque(reinterpret_cast<const native_t*>(ptr));
}

metric_family_hdl* upcast(int_histogram_family_hdl* ptr) {
  using native_t = ct::metric_family_impl<ct::int_histogram>;
  return opaque(reinterpret_cast<native_t*>(ptr));
}

const metric_family_hdl* upcast(const int_histogram_family_hdl* ptr) {
  using native_t = ct::metric_family_impl<ct::int_histogram>;
  return opaque(reinterpret_cast<const native_t*>(ptr));
}

std::string_view prefix(const metric_family_hdl* hdl) noexcept {
  return deref(hdl).prefix();
}

std::string_view name(const metric_family_hdl* hdl) noexcept {
  return deref(hdl).name();
}

span<const std::string> label_names(const metric_family_hdl* hdl) noexcept {
  return deref(hdl).label_names();
}

std::string_view helptext(const metric_family_hdl* hdl) noexcept {
  return deref(hdl).helptext();
}

std::string_view unit(const metric_family_hdl* hdl) noexcept {
  return deref(hdl).unit();
}

bool is_sum(const metric_family_hdl* hdl) noexcept {
  return deref(hdl).is_sum();
}

} // namespace broker::telemetry
