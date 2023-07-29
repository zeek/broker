#include "broker/internal/metric_exporter.hh"

#include <string_view>

#include <caf/actor_system_config.hpp>
#include <caf/config_value.hpp>

#include "broker/defaults.hh"
#include "broker/internal/logger.hh"

namespace broker::internal {

metric_exporter::metric_exporter(caf::actor_system& sys)
  : proc_importer(sys.metrics()) {
  using std::string;
  using dict_type = caf::config_value::dictionary;
  auto& cfg = sys.config();
  if (auto str = caf::get_if<string>(&cfg, "broker.metrics.endpoint-name");
      str && !str->empty()) {
    name = *str;
  }
  if (auto dict = caf::get_if<dict_type>(&cfg, "broker.metrics.export")) {
    if (auto tp = caf::get_if<string>(dict, "topic"); tp && !tp->empty()) {
      target = *tp;
      if (name.empty())
        name = target.suffix();
    }
    interval = caf::get_or(*dict, "interval",
                           defaults::metrics::export_interval);
    if (interval.count() == 0)
      interval = defaults::metrics::export_interval;
    BROKER_INFO("export metrics to topic" << target.string() << "with name"
                                          << name << "every" << interval);
  }
}

[[nodiscard]] bool metric_exporter::valid() const noexcept {
  return !target.empty();
}

} // namespace broker::internal
