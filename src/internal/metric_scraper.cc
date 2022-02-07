#include "broker/internal/metric_scraper.hh"

#include <caf/actor_system_config.hpp>
#include <caf/behavior.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/telemetry/metric.hpp>
#include <caf/telemetry/metric_family.hpp>
#include <caf/telemetry/metric_registry.hpp>

#include "broker/defaults.hh"
#include "broker/detail/assert.hh"
#include "broker/detail/next_tick.hh"
#include "broker/internal/logger.hh"
#include "broker/message.hh"

namespace ct = caf::telemetry;

namespace broker::internal {

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

metric_scraper::metric_scraper(std::string id) : id_(std::move(id)) {
  // nop
}

metric_scraper::metric_scraper(std::vector<std::string> selected_prefixes,
                               std::string id)
  : selected_prefixes_(std::move(selected_prefixes)), id_(std::move(id)) {
  // nop
}

bool metric_scraper::selected(const ct::metric_family* family) {
  auto matches = [family](const auto& prefix) {
    // For now, we only match prefixes on an equality-basis, no globbing.
    return family->prefix() == prefix;
  };
  return selected_prefixes_.empty()
         || std::any_of(selected_prefixes_.begin(), selected_prefixes_.end(),
                        matches);
}

void metric_scraper::scrape(caf::telemetry::metric_registry& registry) {
  last_scrape_ = now();
  if (!rows_.empty()) {
    rows_.resize(1);
    BROKER_ASSERT(is<vector>(rows_[0]));
    auto& meta = get<vector>(rows_[0]);
    BROKER_ASSERT(meta.size() == 2);
    BROKER_ASSERT(is<std::string>(meta[0]));
    BROKER_ASSERT(is<timestamp>(meta[1]));
    get<timestamp>(meta[1]) = last_scrape_;
  } else {
    vector meta;
    meta.emplace_back(id_);
    meta.emplace_back(last_scrape_);
    rows_.emplace_back(std::move(meta));
  }
  BROKER_ASSERT(rows_.size() == 1);
  registry.collect(*this);
}

void metric_scraper::id(std::string new_id) {
  id_ = std::move(new_id);
  rows_.clear(); // Force re-creation of the meta information on next scrape.
}

void metric_scraper::operator()(const ct::metric_family* family,
                                const ct::metric* instance,
                                const ct::dbl_counter* counter) {
  if (selected(family))
    add_row(family, "counter", to_table(instance->labels()), counter->value());
}

void metric_scraper::operator()(const ct::metric_family* family,
                                const ct::metric* instance,
                                const ct::int_counter* counter) {
  if (selected(family))
    add_row(family, "counter", to_table(instance->labels()), counter->value());
}

void metric_scraper::operator()(const ct::metric_family* family,
                                const ct::metric* instance,
                                const ct::dbl_gauge* gauge) {
  if (selected(family))
    add_row(family, "gauge", to_table(instance->labels()), gauge->value());
}

void metric_scraper::operator()(const ct::metric_family* family,
                                const ct::metric* instance,
                                const ct::int_gauge* gauge) {
  if (selected(family))
    add_row(family, "gauge", to_table(instance->labels()), gauge->value());
}

void metric_scraper::operator()(const ct::metric_family* family,
                                const ct::metric* instance,
                                const ct::dbl_histogram* histogram) {
  if (selected(family))
    add_row(family, "histogram", to_table(instance->labels()),
            pack_histogram(histogram));
}

void metric_scraper::operator()(const ct::metric_family* family,
                                const ct::metric* instance,
                                const ct::int_histogram* histogram) {
  if (selected(family))
    add_row(family, "histogram", to_table(instance->labels()),
            pack_histogram(histogram));
}

template <class T>
void metric_scraper::add_row(const caf::telemetry::metric_family* family,
                             std::string type, table labels, T value) {
  vector row;
  row.reserve(8);
  row.emplace_back(family->prefix());
  row.emplace_back(family->name());
  row.emplace_back(std::move(type));
  row.emplace_back(family->unit());
  row.emplace_back(family->helptext());
  row.emplace_back(family->is_sum());
  row.emplace_back(std::move(labels));
  row.emplace_back(std::move(value));
  rows_.emplace_back(std::move(row));
}

} // namespace broker::internal
