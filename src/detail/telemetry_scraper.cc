#include "broker/detail/telemetry_scraper.hh"

#include <caf/actor_system_config.hpp>
#include <caf/behavior.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/telemetry/metric.hpp>
#include <caf/telemetry/metric_registry.hpp>

#include "broker/defaults.hh"
#include "broker/logger.hh"
#include "broker/message.hh"

namespace ct = caf::telemetry;

namespace broker::detail {

namespace {

table to_table(const std::vector<ct::label>& labels) {
  auto str = [](auto str_view) {
    return std::string{str_view.begin(), str_view.end()};
  };
  table result;
  for (const auto& lbl : labels)
    result.emplace(str(lbl.name()), str(lbl.value()));
  return result;
}

// Packs buckets + sum into a Broker vector, where each bucket is a pair of
// numbers (upper bound plus count).
template <class Buckets, class ValueType>
vector pack_histogram(const Buckets& buckets, ValueType sum) {
  vector result;
  result.reserve(buckets.size() + 1); // +1 for the trailing sum
  for (auto& bucket : buckets) {
    vector packed_bucket;
    packed_bucket.reserve(2);
    packed_bucket.emplace_back(bucket.upper_bound);
    packed_bucket.emplace_back(bucket.count.value());
    result.emplace_back(std::move(packed_bucket));
  }
  result.emplace_back(sum);
  return result;
}

template <class Histogram>
vector pack_histogram(const Histogram* ptr) {
  return pack_histogram(ptr->buckets(), ptr->sum());
}

} // namespace

// -- initialization -----------------------------------------------------------

telemetry_scraper_state::telemetry_scraper_state(
  caf::event_based_actor* self, caf::actor core,
  std::vector<std::string> selected_prefixes, caf::timespan interval,
  topic target)
  : self(self),
    core(std::move(core)),
    selected_prefixes(std::move(selected_prefixes)),
    interval(interval),
    target(std::move(target)) {
  // nop
}

telemetry_scraper_state::telemetry_scraper_state(caf::event_based_actor* self,
                                                 caf::actor core, topic target)
  : self(self), core(std::move(core)), target(std::move(target)) {
  using svec = std::vector<std::string>;
  const auto& cfg = self->system().config();
  if (auto filter = caf::get_as<svec>(cfg, "broker.metrics-publish-filter"))
    selected_prefixes = std::move(*filter);
  interval = caf::get_or(cfg, "broker.metrics-publish-interval",
                         defaults::metrics_publish_interval);
}

caf::behavior telemetry_scraper_state::make_behavior() {
  if (!core) {
    BROKER_WARNING("spawned a telemetry scraper with invalid core handle");
    return {};
  }
  self->monitor(core);
  self->set_down_handler([this](const caf::down_msg& down) {
    if (down.source == core)
      self->quit(down.reason);
  });
  if (interval.count() > 0) {
    last_scrape = self->clock().now();
    self->scheduled_send(self, last_scrape + interval, caf::tick_atom_v);
  }
  return {
    [this](caf::tick_atom) {
      scrape();
      if (!tbl.empty())
        self->send(core, atom::publish_v, make_data_message(target, tbl));
      last_scrape += interval;
      auto next_scrape = last_scrape + interval;
      auto now = self->clock().now();
      while (now >= next_scrape)
        next_scrape += interval;
      self->scheduled_send(self, next_scrape, caf::tick_atom_v);
    },
  };
}

// -- callbacks for the metric registry ----------------------------------------

void telemetry_scraper_state::operator()(const ct::metric_family* family,
                                         const ct::metric* instance,
                                         const ct::dbl_counter* counter) {
  if (selected(family))
    add_row(family, "counter", to_table(instance->labels()), counter->value());
}

void telemetry_scraper_state::operator()(const ct::metric_family* family,
                                         const ct::metric* instance,
                                         const ct::int_counter* counter) {
  if (selected(family))
    add_row(family, "counter", to_table(instance->labels()), counter->value());
}

void telemetry_scraper_state::operator()(const ct::metric_family* family,
                                         const ct::metric* instance,
                                         const ct::dbl_gauge* gauge) {
  if (selected(family))
    add_row(family, "gauge", to_table(instance->labels()), gauge->value());
}

void telemetry_scraper_state::operator()(const ct::metric_family* family,
                                         const ct::metric* instance,
                                         const ct::int_gauge* gauge) {
  if (selected(family))
    add_row(family, "gauge", to_table(instance->labels()), gauge->value());
}

void telemetry_scraper_state::operator()(const ct::metric_family* family,
                                         const ct::metric* instance,
                                         const ct::dbl_histogram* histogram) {
  if (selected(family))
    add_row(family, "histogram", to_table(instance->labels()),
            pack_histogram(histogram));
}

void telemetry_scraper_state::operator()(const ct::metric_family* family,
                                         const ct::metric* instance,
                                         const ct::int_histogram* histogram) {
  if (selected(family))
    add_row(family, "histogram", to_table(instance->labels()),
            pack_histogram(histogram));
}

// -- utility functions --------------------------------------------------------

void telemetry_scraper_state::scrape() {
  tbl.clear();
  self->system().metrics().collect(*this);
}

bool telemetry_scraper_state::selected(const ct::metric_family* family) {
  auto matches = [family](const auto& prefix) {
    // For now, we only match prefixes on an equality-basis, no globbing.
    return family->prefix() == prefix;
  };
  return selected_prefixes.empty()
         || std::any_of(selected_prefixes.begin(), selected_prefixes.end(),
                        matches);
}

} // namespace broker::detail
