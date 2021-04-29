#include "broker/detail/telemetry/exporter.hh"

#include <string_view>

#include <caf/actor_system_config.hpp>
#include <caf/config_value.hpp>

#include "broker/defaults.hh"

namespace broker::detail::telemetry {

optional<exporter_params>
exporter_params::from(const caf::actor_system_config& cfg) {
  using dict_type = caf::config_value::dictionary;
  if (auto dict = caf::get_if<dict_type>(&cfg, "broker.metrics.export")) {
    if (auto tp = caf::get_if<std::string>(dict, "topic"); tp && !tp->empty()) {
      auto t = topic{*tp};
      auto id = caf::get_or(cfg, "broker.metrics.endpoint-name",
                            caf::string_view{t.suffix()});
      auto interval = caf::get_or(*dict, "interval",
                                  defaults::metrics::export_interval);
      if (interval.count() == 0)
        interval = defaults::metrics::export_interval;
      return exporter_params{
        caf::get_or(*dict, "prefixes", std::vector<std::string>{}),
        interval,
        std::move(t),
        std::move(id),
      };
    }
  }
  return {};
}

} // namespace broker::detail::telemetry
