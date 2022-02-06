#include "broker/telemetry/metric_registry_impl.hh"

namespace broker::telemetry {

metric_registry_impl::metric_registry_impl() : rc_(1) {
  // nop
}

metric_registry_impl::~metric_registry_impl() {
  // nop
}

} // namespace broker::telemetry
