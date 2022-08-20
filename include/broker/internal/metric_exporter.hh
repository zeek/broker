#pragma once

#include <optional>
#include <string>
#include <vector>

#include <caf/actor.hpp>
#include <caf/actor_clock.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/fwd.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/telemetry/importer/process.hpp>

#include "broker/detail/next_tick.hh"
#include "broker/filter_type.hh"
#include "broker/internal/logger.hh"
#include "broker/internal/metric_scraper.hh"
#include "broker/internal/type_id.hh"
#include "broker/message.hh"
#include "broker/topic.hh"

namespace broker::internal {

/// Wraps user-defined parameters for the exporter to enable sanity checking
/// before actually spawning an exporter.
struct metric_exporter_params {
  std::vector<std::string> selected_prefixes;
  caf::timespan interval = caf::timespan{0};
  topic target;
  std::string id;
  static metric_exporter_params from(const caf::actor_system_config& cfg);
  [[nodiscard]] bool valid() const noexcept;
};

/// State for an actor that periodically exports local metrics by publishing
/// `data`-encoded metrics to a user-defined Broker topic.
template <class Self>
class metric_exporter_state {
public:
  // -- initialization ---------------------------------------------------------

  metric_exporter_state(Self* self, caf::actor core,
                        std::vector<std::string> selected_prefixes,
                        caf::timespan interval, topic target, std::string id)
    : self(self),
      core(std::move(core)),
      interval(interval),
      target(std::move(target)),
      proc_importer(self->system().metrics()),
      impl(std::move(selected_prefixes), std::move(id)) {
    // nop
  }

  metric_exporter_state(Self* self, caf::actor core,
                        metric_exporter_params&& params)
    : metric_exporter_state(self, std::move(core),
                            std::move(params.selected_prefixes),
                            params.interval, std::move(params.target),
                            std::move(params.id)) {
    // nop
  }

  caf::behavior make_behavior() {
    BROKER_ASSERT(core);
    self->monitor(core);
    self->set_down_handler([this](const caf::down_msg& down) {
      if (down.source == core) {
        BROKER_INFO(self->name()
                    << "received a down message from the core: bye");
        self->quit(down.reason);
      }
    });
    cold_boot();
    return {
      [this](caf::tick_atom) {
        if (running_) {
          proc_importer.update();
          impl.scrape(self->system().metrics());
          // Send nothing if we only have meta data (or nothing) to send.
          if (const auto& rows = impl.rows(); rows.size() > 1)
            self->send(core, atom::publish_v, make_data_message(target, rows));
          auto t = detail::next_tick(tick_init, self->clock().now(), interval);
          self->scheduled_send(self, t, caf::tick_atom_v);
        }
      },
      [this](atom::put, caf::timespan new_interval) {
        set_interval(new_interval);
      },
      [this](atom::put, topic& new_target) {
        set_target(std::move(new_target));
      },
      [this](atom::put, std::string& new_id) { set_id(std::move(new_id)); },
      [this](atom::put, filter_type& new_prefixes_filter) {
        set_prefixes(std::move(new_prefixes_filter));
      },
      [this](atom::join, const filter_type&) {
        // Ignored. The prometheus_actor "overrides" this handler but it has no
        // effect on a "plain" exporter.
      },
    };
  }

  // -- utility functions ------------------------------------------------------

  [[nodiscard]] bool has_valid_config() const noexcept {
    return !target.empty();
  }

  [[nodiscard]] bool running() const noexcept {
    return running_;
  }

  /// Starts the timed loop if the exporter hasn't been running yet and all
  /// preconditions are met.
  void cold_boot() {
    if (!running_ && has_valid_config()) {
      BROKER_INFO("start publishing metrics to topic" << target);
      impl.scrape(self->system().metrics());
      tick_init = self->clock().now();
      self->scheduled_send(self, tick_init + interval, caf::tick_atom_v);
      running_ = true;
    }
  }

  void set_interval(caf::timespan new_interval) {
    if (new_interval.count() > 0) {
      // Use the new interval from the next tick onward.
      if (running_)
        tick_init = detail::next_tick(tick_init, self->clock().now(), interval);
      interval = new_interval;
      cold_boot();
    }
  }

  void set_target(topic new_target) {
    if (!new_target.empty()) {
      BROKER_INFO("publish metrics to topic" << new_target);
      target = std::move(new_target);
      if (impl.id().empty())
        impl.id(std::string{target.suffix()});
      cold_boot();
    }
  }

  void set_id(std::string new_id) {
    if (!new_id.empty()) {
      impl.id(std::move(new_id));
      cold_boot();
    }
  }

  void set_prefixes(filter_type new_prefixes_filter) {
    // We only wrap the prefixes into a filter to get around assigning a type ID
    // to std::vector<std::string> (which technically would require us to change
    // Broker ID on the network).
    std::vector<std::string> new_prefixes;
    for (auto& prefix : new_prefixes_filter)
      new_prefixes.emplace_back(std::move(prefix).move_string());
    impl.selected_prefixes(std::move(new_prefixes));
    cold_boot();
  }

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

  /// Adds metrics for CPU and RAM usage.
  caf::telemetry::importer::process proc_importer;

  /// The actual exporter for collecting the metrics.
  metric_scraper impl;

  bool running_ = false;

  static inline const char* name = "broker.exporter";
};

using metric_exporter_actor =
  caf::stateful_actor<metric_exporter_state<caf::event_based_actor>>;

} // namespace broker::internal
