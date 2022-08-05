#include "broker/telemetry/metric_family.hh"

#include <caf/telemetry/counter.hpp>
#include <caf/telemetry/gauge.hpp>
#include <caf/telemetry/histogram.hpp>
#include <caf/telemetry/metric_family.hpp>
#include <caf/telemetry/metric_family_impl.hpp>

namespace ct = caf::telemetry;

namespace broker::telemetry {

namespace {

template <class T>
struct native_hdl_oracle;

// Generates the implementation for a `native_hdl_oracle` specialization.
#define GEN_NATIVE_HDL_ORACLE(BrokerType, NativeType)                          \
  template <>                                                                  \
  struct native_hdl_oracle<BrokerType> {                                       \
    using type = NativeType;                                                   \
  };

GEN_NATIVE_HDL_ORACLE(dbl_counter_family_hdl,
                      ct::metric_family_impl<ct::dbl_counter>)

GEN_NATIVE_HDL_ORACLE(dbl_gauge_family_hdl,
                      ct::metric_family_impl<ct::dbl_gauge>)

GEN_NATIVE_HDL_ORACLE(dbl_histogram_family_hdl,
                      ct::metric_family_impl<ct::dbl_histogram>)

GEN_NATIVE_HDL_ORACLE(int_counter_family_hdl,
                      ct::metric_family_impl<ct::int_counter>)

GEN_NATIVE_HDL_ORACLE(int_gauge_family_hdl,
                      ct::metric_family_impl<ct::int_gauge>)

GEN_NATIVE_HDL_ORACLE(int_histogram_family_hdl,
                      ct::metric_family_impl<ct::int_histogram>)

template <class T>
using native_hdl_t = typename native_hdl_oracle<T>::type;

auto& deref(const metric_family_hdl* hdl) {
  return *reinterpret_cast<const ct::metric_family*>(hdl);
}

template <class T>
metric_family_hdl* upcast_impl(T* ptr) {
  using native_ptr_t = native_hdl_t<T>*;
  ct::metric_family* native_ptr = reinterpret_cast<native_ptr_t>(ptr);
  return reinterpret_cast<metric_family_hdl*>(native_ptr);
}

template <class T>
const metric_family_hdl* upcast_impl(const T* ptr) {
  using native_ptr_t = const native_hdl_t<T>*;
  const ct::metric_family* native_ptr = reinterpret_cast<native_ptr_t>(ptr);
  return reinterpret_cast<const metric_family_hdl*>(native_ptr);
}

template <class T>
T* downcast_impl(metric_family_hdl* ptr) {
  auto native_ptr = reinterpret_cast<ct::metric_family*>(ptr);
  auto native_dptr = static_cast<native_hdl_t<T>*>(native_ptr);
  return reinterpret_cast<T*>(native_dptr);
}

template <class T>
const T* downcast_impl(const metric_family_hdl* ptr) {
  auto native_ptr = reinterpret_cast<const ct::metric_family*>(ptr);
  auto native_dptr = static_cast<const native_hdl_t<T>*>(native_ptr);
  return reinterpret_cast<const T*>(native_dptr);
}

} // namespace

// Generates the implementations (const and non-const) for the `upcast`
// overloads and for the `as_...` overloads.
#define GEN_CASTS(BrokerType)                                                  \
  metric_family_hdl* upcast(BrokerType##_hdl* ptr) {                           \
    return upcast_impl(ptr);                                                   \
  }                                                                            \
  const metric_family_hdl* upcast(const BrokerType##_hdl* ptr) {               \
    return upcast_impl(ptr);                                                   \
  }                                                                            \
  BrokerType##_hdl* as_##BrokerType(metric_family_hdl* ptr) {                  \
    return downcast_impl<BrokerType##_hdl>(ptr);                               \
  }                                                                            \
  const BrokerType##_hdl* as_##BrokerType(const metric_family_hdl* ptr) {      \
    return downcast_impl<BrokerType##_hdl>(ptr);                               \
  }

GEN_CASTS(dbl_counter_family)

GEN_CASTS(dbl_gauge_family)

GEN_CASTS(dbl_histogram_family)

GEN_CASTS(int_counter_family)

GEN_CASTS(int_gauge_family)

GEN_CASTS(int_histogram_family)

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
