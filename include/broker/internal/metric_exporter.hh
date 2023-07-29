#pragma once

#include "broker/detail/next_tick.hh"
#include "broker/filter_type.hh"
#include "broker/internal/logger.hh"
#include "broker/internal/type_id.hh"
#include "broker/message.hh"
#include "broker/topic.hh"

#include "broker/internal/logger.hh"

#include <caf/actor.hpp>
#include <caf/actor_clock.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/fwd.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/telemetry/collector/prometheus.hpp>
#include <caf/telemetry/importer/process.hpp>

#include <optional>
#include <string>
#include <vector>

namespace broker::internal {

/// Wraps user-defined parameters for the exporter to enable sanity checking
/// before actually spawning an exporter.
struct metric_exporter {
  explicit metric_exporter(caf::actor_system& sys);

  /// Adds metrics for CPU and RAM usage.
  caf::telemetry::importer::process proc_importer;

  /// Collects metrics from the local Broker instance.
  caf::telemetry::collector::prometheus collector;

  /// The interval between two exports.
  caf::timespan interval;

  /// The target topic when exporting metrics to remote Broker instances.
  topic target;

  /// The name of the local Broker endpoint (`broker.metrics.endpoint-name`).
  std::string name;

  /// The time point when the exporter was started.
  caf::actor_clock::time_point tick_init;

  /// Parses the exporter parameters from the given config.
  bool init(const caf::actor_system_config& cfg);

  /// Returns whether the exporter is in a valid state.
  [[nodiscard]] bool valid() const noexcept;

  template<class Self>
  void schedule_first_tick(Self* self) {
    tick_init = self->clock().now();
    if (interval.count() > 0)
      self->scheduled_send(self, tick_init + interval, caf::tick_atom_v);
  }

  /// Handles a tick event by collecting metrics and publishing them.
  template <class Self>
  void on_tick(Self* self, const caf::actor& core) {
    // Update process metrics.
    proc_importer.update();
    // Collect metrics and publish them.
    auto res = collector.collect_from(self->system().metrics());
    vector rows;
    rows.reserve(3);
    rows.emplace_back(name);
    rows.emplace_back(broker::now());
    rows.emplace_back(std::string{res.begin(), res.end()});
    self->send(core, atom::publish_v,
               make_data_message(target, data{std::move(rows)}));
    // Schedule next tick.
    auto t = detail::next_tick(tick_init, self->clock().now(), interval);
    self->scheduled_send(self, t, caf::tick_atom_v);
  }
};

/// State for an actor that wraps a metric_exporter.
class metric_exporter_state {
public:
  // -- initialization ---------------------------------------------------------

  metric_exporter_state(caf::event_based_actor* self, caf::actor core)
    : self(self), core(std::move(core)), impl(self->system()) {
    // nop
  }

  caf::behavior make_behavior() {
    BROKER_ASSERT(core);
    if (!impl.valid()) {
      BROKER_INFO("invalid metric exporter configuration: stop exporter");
      return {};
    }
    impl.schedule_first_tick(self);
    self->monitor(core);
    self->set_down_handler([this](const caf::down_msg& down) {
      if (down.source == core) {
        BROKER_INFO(self->name()
                    << "received a down message from the core: bye");
        self->quit(down.reason);
      }
    });
    return {
      [this](caf::tick_atom) { impl.on_tick(self, core); },
      [this](atom::put, caf::timespan new_interval) {
        impl.interval = new_interval;
      },
      [this](atom::put, topic& new_target) {
        impl.target = std::move(new_target);
      },
      [this](atom::put, std::string& new_name) {
        impl.name = std::move(new_name);
      },
      [this](atom::put, filter_type&) {
        // nop
      },
      [this](atom::join, const filter_type&) {
        // Ignored. The prometheus_actor "overrides" this handler but it has no
        // effect on a "plain" exporter.
      },
    };
  }

  // -- member variables -------------------------------------------------------

  /// Points to the actor (exporter) that owns this state object.
  caf::event_based_actor* self;

  /// Handle to the core actor.
  caf::actor core;

  /// Implements the Prometheus scraping logic.
  metric_exporter impl;

  bool running_ = false;

  static inline const char* name = "broker.exporter";
};

using metric_exporter_actor = caf::stateful_actor<metric_exporter_state>;

} // namespace broker::internal
