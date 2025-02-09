#pragma once

#include "broker/fwd.hh"

#include <prometheus/family.h>
#include <prometheus/histogram.h>
#include <prometheus/labels.h>
#include <prometheus/metric_family.h>
#include <prometheus/registry.h>

#include <string>

namespace broker::internal {

/// Provides a single access point for all Broker metric families and instances.
class metric_factory {
public:
  // -- convenience type aliases -----------------------------------------------

  template <class T>
  using family_t = prometheus::Family<T>;

  using gauge = prometheus::Gauge;

  using gauge_family = family_t<gauge>;

  using counter = prometheus::Counter;

  using counter_family = family_t<counter>;

  using histogram = prometheus::Histogram;

  using histogram_family = family_t<histogram>;

  /// Bundles all Broker metrics for the core system.
  class core_t {
  public:
    explicit core_t(prometheus::Registry& reg) : reg_(&reg) {}

    core_t(const core_t&) noexcept = default;

    core_t& operator=(const core_t&) noexcept = default;

    /// Keeps track of the active connections.
    ///
    /// Label dimensions: `type` ('native' or 'web-socket').
    gauge_family* connections_family();

    struct connections_t {
      gauge* native;
      gauge* web_socket;
    };

    /// Returns all instances of `broker.connections`.
    connections_t connections_instances();

    /// Counts how many messages Broker has processed in total per message type.
    ///
    /// Label dimensions: `type` ('data', 'command', 'routing-update', 'ping',
    /// or 'pong').
    counter_family* processed_messages_family();

    struct processed_messages_t {
      counter* data;
      counter* command;
      counter* routing_update;
      counter* ping;
      counter* pong;
    };

    /// Returns all instances of `broker.processed-messages`.
    processed_messages_t processed_messages_instances();

  private:
    prometheus::Registry* reg_;
  };

  /// Bundles all Broker metrics for the data stores.
  class store_t {
  public:
    explicit store_t(prometheus::Registry& reg) : reg_(&reg) {}

    store_t(const store_t&) noexcept = default;

    store_t& operator=(const store_t&) noexcept = default;

    /// Counts how many input channels a data store currently has.
    gauge_family* input_channels_family();

    /// Returns an instance of `broker.store-input-channels` for the given
    /// data store.
    gauge* input_channels_instance(std::string name);

    /// Counts how many inputs are currently buffered because they arrived
    /// out-of-order.
    gauge_family* out_of_order_updates_family();

    /// Returns an instance of `broker.store-out-of-order-updates` for the given
    /// data store.
    gauge* out_of_order_updates_instance(std::string name);

    /// Counts how many output channels a data store currently has.
    gauge_family* output_channels_family();

    /// Returns an instance of `broker.store-output-channels` for the given
    /// data store.
    gauge* output_channels_instance(std::string name);

    /// Counts how many entries a data store currently has.
    gauge_family* entries_family();

    /// Returns an instance of `broker.store-entries` for the given data store.
    gauge* entries_instance(std::string name);

    /// Counts how many updates were processed in total.
    counter_family* processed_updates_family();

    /// Returns an instance of `broker.store-processed-updates` for the
    /// given data store.
    counter* processed_updates_instance(std::string name);

    /// Counts how many updates are currently unacknowledged.
    gauge_family* unacknowledged_updates_family();

    /// Returns an instance of `broker.store-unacknowledged-updates` for the
    /// given data store.
    gauge* unacknowledged_updates_instance(std::string name);

  private:
    prometheus::Registry* reg_;
  };

  // -- properties -------------------------------------------------------------

  core_t core;

  store_t store;

  // --- constructors ----------------------------------------------------------

  explicit metric_factory(prometheus::Registry& reg) noexcept
    : core(reg), store(reg) {
    // nop
  }

  metric_factory(const metric_factory&) noexcept = default;

  metric_factory& operator=(const metric_factory&) noexcept = default;
};

} // namespace broker::internal
