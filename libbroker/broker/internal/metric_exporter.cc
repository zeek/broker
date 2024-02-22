#include "broker/internal/metric_exporter.hh"

#include <string_view>

#include <caf/actor_system_config.hpp>
#include <caf/config_value.hpp>

#include "broker/defaults.hh"

namespace broker::internal {

metric_exporter_params
metric_exporter_params::from(const caf::actor_system_config& cfg) {
  using std::string;
  using dict_type = caf::config_value::dictionary;
  metric_exporter_params result;
  if (auto idp = caf::get_if<string>(&cfg, "broker.metrics.endpoint-name");
      idp && !idp->empty()) {
    result.id = *idp;
  }
  if (auto dict = caf::get_if<dict_type>(&cfg, "broker.metrics.export")) {
    if (auto tp = caf::get_if<string>(dict, "topic"); tp && !tp->empty()) {
      result.target = *tp;
      if (result.id.empty())
        result.id = result.target.suffix();
    }
    result.interval = caf::get_or(*dict, "interval",
                                  defaults::metrics::export_interval);
    if (result.interval.count() == 0)
      result.interval = defaults::metrics::export_interval;
  }
  return result;
}

[[nodiscard]] bool metric_exporter_params::valid() const noexcept {
  return !target.empty();
}

} // namespace broker::internal
