#pragma once

#include <string>
#include <vector>

#include <caf/actor.hpp>
#include <caf/actor_clock.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/fwd.hpp>
#include <caf/stateful_actor.hpp>

#include "broker/detail/next_tick.hh"
#include "broker/detail/telemetry/scraper.hh"
#include "broker/logger.hh"
#include "broker/message.hh"
#include "broker/optional.hh"
#include "broker/topic.hh"

namespace broker::detail::telemetry {

/// Wraps user-defined parameters for the exporter to enable sanity checking
/// before actually spawning an exporter.
struct exporter_params {
  std::vector<std::string> selected_prefixes;
  caf::timespan interval;
  topic target;
  std::string id;
  static optional<exporter_params> from(const caf::actor_system_config& cfg);
};

/// State for an actor that periodically exports local metrics by publishing
/// `data`-encoded metrics to a user-defined Broker topic.
template <class Self>
class exporter_state {
public:
  // -- initialization ---------------------------------------------------------

  exporter_state(Self* self, caf::actor core,
                 std::vector<std::string> selected_prefixes,
                 caf::timespan interval, topic target, std::string id)
    : self(self),
      core(std::move(core)),
      interval(interval),
      target(std::move(target)),
      impl(std::move(selected_prefixes), std::move(id)) {
    // nop
  }

  exporter_state(Self* self, caf::actor core, exporter_params&& params)
    : exporter_state(self, std::move(core), std::move(params.selected_prefixes),
                     params.interval, std::move(params.target),
                     std::move(params.id)) {
    //nop
  }

  caf::behavior make_behavior() {
    BROKER_ASSERT(core != nullptr);
    BROKER_ASSERT(interval.count() > 0);
    self->monitor(core);
    self->set_down_handler([this](const caf::down_msg& down) {
      if (down.source == core) {
        BROKER_INFO(self->name()
                    << "received a down message from the core: bye");
        self->quit(down.reason);
      }
    });
    tick_init = self->clock().now();
    self->scheduled_send(self, tick_init + interval, caf::tick_atom_v);
    return {
      [this](caf::tick_atom) {
        impl.scrape(self->system().metrics());
        // Send nothing if we only have meta data (or nothing) to send.
        if (const auto& rows = impl.rows(); rows.size() > 1)
          self->send(core, atom::publish_v, make_data_message(target, rows));
        auto t = next_tick(tick_init, self->clock().now(), interval);
        self->scheduled_send(self, t, caf::tick_atom_v);
      },
    };
  }

  // -- utility functions ------------------------------------------------------

  /// Encodes selected metrics from the CAF metrics registry to a table-like
  /// data structure and stores the result in `rows`.
  void scrape();

  // -- member variables -------------------------------------------------------

  /// Points to the actor (exporter) that owns this state object.
  Self* self;

  /// Handle to the core actor.
  caf::actor core;

  /// Configures how frequent the exporter collects metrics from the system.
  caf::timespan interval;

  /// Caches the time point of our initialization for scheduling ticks.
  caf::actor_clock::time_point tick_init;

  /// Configures the topic for periodically publishing scrape results to.
  topic target;

  /// Configures the ID of this exporter to allow subscribers to disambiguate
  /// remote metrics. By default, the exporter uses the target suffix as ID. For
  /// example, constructing the exporter with target topic
  /// `/zeek/metrics/worker-1` would set this field to "worker-1".
  std::string id;

  /// The actual exporter for collecting the metrics.
  scraper impl;
};

using exporter_actor
  = caf::stateful_actor<exporter_state<caf::event_based_actor>>;

} // namespace broker::detail::telemetry
