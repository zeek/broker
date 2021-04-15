#pragma once

#include <caf/actor.hpp>
#include <caf/actor_clock.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/telemetry/metric_family.hpp>

#include "broker/data.hh"
#include "broker/topic.hh"

namespace broker::detail {

struct telemetry_scraper_state {
  // -- initialization ---------------------------------------------------------

  telemetry_scraper_state(caf::event_based_actor* self, caf::actor core,
                          std::vector<std::string> selected_prefixes,
                          caf::timespan interval, topic target);

  telemetry_scraper_state(caf::event_based_actor* self, caf::actor core,
                          topic target);

  caf::behavior make_behavior();

  // -- callbacks for the metric registry --------------------------------------

  void operator()(const caf::telemetry::metric_family* family,
                  const caf::telemetry::metric* instance,
                  const caf::telemetry::dbl_counter* counter);

  void operator()(const caf::telemetry::metric_family* family,
                  const caf::telemetry::metric* instance,
                  const caf::telemetry::int_counter* counter);

  void operator()(const caf::telemetry::metric_family* family,
                  const caf::telemetry::metric* instance,
                  const caf::telemetry::dbl_gauge* gauge);

  void operator()(const caf::telemetry::metric_family* family,
                  const caf::telemetry::metric* instance,
                  const caf::telemetry::int_gauge* gauge);

  template <class T>
  void operator()(const caf::telemetry::metric_family*,
                  const caf::telemetry::metric*, const T*) {
    // nop
  }

  // -- utility functions ------------------------------------------------------

  void scrape();

  bool selected(const caf::telemetry::metric_family* family);

  template <class T>
  void add_row(const caf::telemetry::metric_family* family, std::string type,
               table labels, T value) {
    vector row;
    row.reserve(5);
    row.emplace_back(family->prefix());
    row.emplace_back(family->name());
    row.emplace_back(std::move(type));
    row.emplace_back(std::move(labels));
    row.emplace_back(std::move(value));
    tbl.emplace_back(std::move(row));
  }

  // -- member variables -------------------------------------------------------

  /// Points to the actor (scraper) that owns this state object.
  caf::event_based_actor* self;

  /// Handle to the core actor.
  caf::actor core;

  /// Configures which metrics the scraper includes.
  std::vector<std::string> selected_prefixes;

  /// Configures how frequent the scraper collects metrics from the system.
  caf::timespan interval;

  /// Caches the time point of our last scrape for publishing at regular time
  /// points.
  caf::actor_clock::time_point last_scrape;

  /// Configures the topic for periodically publishing scrape results to.
  topic target;

  /// Contains the result for the last scraping run.
  vector tbl;
};

using telemetry_scraper_actor = caf::stateful_actor<telemetry_scraper_state>;

} // namespace broker::detail
