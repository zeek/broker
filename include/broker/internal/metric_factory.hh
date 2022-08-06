#pragma once

#include <caf/fwd.hpp>
#include <caf/telemetry/counter.hpp>
#include <caf/telemetry/gauge.hpp>
#include <caf/telemetry/histogram.hpp>
#include <caf/telemetry/metric_registry.hpp>

namespace broker::internal {

/// Provides a single access point for all Broker metric families and instances.
class metric_factory {
public:
  // -- convenience type aliases -----------------------------------------------

  template <class T>
  using family_t = caf::telemetry::metric_family_impl<T>;

  using dbl_gauge = caf::telemetry::dbl_gauge;

  using dbl_gauge_family = family_t<dbl_gauge>;

  using int_gauge = caf::telemetry::int_gauge;

  using int_gauge_family = family_t<int_gauge>;

  using dbl_counter = caf::telemetry::dbl_counter;

  using dbl_counter_family = family_t<dbl_counter>;

  using int_counter = caf::telemetry::int_counter;

  using int_counter_family = family_t<int_counter>;

  using dbl_histogram = caf::telemetry::dbl_histogram;

  using dbl_histogram_family = family_t<dbl_histogram>;

  using int_histogram = caf::telemetry::int_histogram;

  using int_histogram_family = family_t<int_histogram>;

  /// Bundles all Broker metrics for the core system.
  class core_t {
  public:
    explicit core_t(caf::telemetry::metric_registry* reg) : reg_(reg) {}

    core_t(const core_t&) noexcept = default;

    core_t& operator=(const core_t&) noexcept = default;

    /// Keeps track of the active connections.
    ///
    /// Label dimensions: `type` ('native' or 'web-socket').
    int_gauge_family* connections_family();

    struct connections_t {
      int_gauge* native;
      int_gauge* web_socket;
    };

    /// Returns all instances of `broker.connections`.
    connections_t connections_instances();

    /// Counts how many messages Broker has processed in total per message type.
    ///
    /// Label dimensions: `type` ('data', 'command', 'routing-update', 'ping',
    /// or 'pong').
    int_counter_family* processed_messages_family();

    struct processed_messages_t {
      int_counter* data;
      int_counter* command;
      int_counter* routing_update;
      int_counter* ping;
      int_counter* pong;
    };

    /// Returns all instances of `broker.processed-messages`.
    processed_messages_t processed_messages_instances();

    /// Counts how many messages Broker has buffered in total per message type.
    ///
    /// Label dimensions: `type` ('data', 'command', 'routing-update', 'ping',
    /// or 'pong').
    int_gauge_family* buffered_messages_family();

    struct buffered_messages_t {
      int_gauge* data;
      int_gauge* command;
      int_gauge* routing_update;
      int_gauge* ping;
      int_gauge* pong;
    };

    /// Returns all instances of `broker.buffered-messages`.
    buffered_messages_t buffered_messages_instances();

  private:
    caf::telemetry::metric_registry* reg_;
  };

  /// Bundles all Broker metrics for the data stores.
  class store_t {
  public:
    explicit store_t(caf::telemetry::metric_registry* reg) : reg_(reg) {}

    store_t(const store_t&) noexcept = default;

    store_t& operator=(const store_t&) noexcept = default;

    /// Counts how many input channels a data store currently has.
    int_gauge_family* input_channels_family();

    /// Returns an instance of `broker.store-input-channels` for the given
    /// data store.
    int_gauge* input_channels_instance(std::string_view name);

    /// Counts how many inputs are currently buffered because they arrived
    /// out-of-order.
    int_gauge_family* out_of_order_updates_family();

    /// Returns an instance of `broker.store-out-of-order-updates` for the given
    /// data store.
    int_gauge* out_of_order_updates_instance(std::string_view name);

    /// Counts how many output channels a data store currently has.
    int_gauge_family* output_channels_family();

    /// Returns an instance of `broker.store-output-channels` for the given
    /// data store.
    int_gauge* output_channels_instance(std::string_view name);

    /// Counts how many entries a data store currently has.
    int_gauge_family* entries_family();

    /// Returns an instance of `broker.store-entries` for the given data store.
    int_gauge* entries_instance(std::string_view name);

    /// Counts how many updates were processed in total.
    int_counter_family* processed_updates_family();

    /// Returns an instance of `broker.store-processed-updates` for the
    /// given data store.
    int_counter* processed_updates_instance(std::string_view name);

    /// Counts how many updates are currently unacknowledged.
    int_gauge_family* unacknowledged_updates_family();

    /// Returns an instance of `broker.store-unacknowledged-updates` for the
    /// given data store.
    int_gauge* unacknowledged_updates_instance(std::string_view name);

  private:
    caf::telemetry::metric_registry* reg_;
  };

  // -- properties -------------------------------------------------------------

  core_t core;

  store_t store;

  // --- constructors ----------------------------------------------------------

  explicit metric_factory(caf::actor_system& sys) noexcept;

  explicit metric_factory(caf::telemetry::metric_registry& reg) noexcept
    : core(&reg), store(&reg) {
    // nop
  }

  metric_factory(const metric_factory&) noexcept = default;

  metric_factory& operator=(const metric_factory&) noexcept = default;
};

} // namespace broker::internal
