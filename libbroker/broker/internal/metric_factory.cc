#include "broker/internal/metric_factory.hh"

#include <caf/actor_system.hpp>

namespace broker::internal {

// -- 'imports' to safe ourselves some typing ----------------------------------

using gauge = metric_factory::gauge;

using gauge_family = metric_factory::gauge_family;

using counter = metric_factory::counter;

using counter_family = metric_factory::counter_family;

using histogram = metric_factory::histogram;

using histogram_family = metric_factory::histogram_family;

// -- core metrics -------------------------------------------------------------

using core_t = metric_factory::core_t;

gauge_family* core_t::connections_family() {
  return &prometheus::BuildGauge()
            .Name("broker_connections")
            .Help("Number of active network connections.")
            .Register(*reg_);
}

core_t::connections_t core_t::connections_instances() {
  auto fm = connections_family();
  return {
    .native = &fm->Add({{"type", "native"}}),
    .web_socket = &fm->Add({{"type", "web-socket"}}),
  };
}

counter_family* core_t::processed_messages_family() {
  return &prometheus::BuildCounter()
            .Name("broker_processed_messages_total")
            .Help("Total number of processed messages.")
            .Register(*reg_);
}

core_t::processed_messages_t core_t::processed_messages_instances() {
  auto fm = processed_messages_family();
  return {
    .data = &fm->Add({{"type", "data"}}),
    .command = &fm->Add({{"type", "command"}}),
    .routing_update = &fm->Add({{"type", "routing-update"}}),
    .ping = &fm->Add({{"type", "ping"}}),
    .pong = &fm->Add({{"type", "pong"}}),
  };
}

// -- store metrics ------------------------------------------------------------

using store_t = metric_factory::store_t;

gauge_family* store_t::input_channels_family() {
  return &prometheus::BuildGauge()
            .Name("broker_store_input_channels")
            .Help("Number of active input channels in a data store.")
            .Register(*reg_);
}

gauge* store_t::input_channels_instance(std::string name) {
  return &input_channels_family()->Add({{"name", std::move(name)}});
}

gauge_family* store_t::out_of_order_updates_family() {
  return &prometheus::BuildGauge()
            .Name("broker_store_out_of_order_updates")
            .Help("Number of out-of-order data store updates.")
            .Register(*reg_);
}

gauge* store_t::out_of_order_updates_instance(std::string name) {
  return &out_of_order_updates_family()->Add({{"name", std::move(name)}});
}

gauge_family* store_t::output_channels_family() {
  return &prometheus::BuildGauge()
            .Name("broker_store_output_channels")
            .Help("Number of active output channels in a data store.")
            .Register(*reg_);
}

gauge* store_t::output_channels_instance(std::string name) {
  return &output_channels_family()->Add({{"name", std::move(name)}});
}

gauge_family* store_t::entries_family() {
  return &prometheus::BuildGauge()
            .Name("broker_store_entries")
            .Help("Number of entries in the data store.")
            .Register(*reg_);
}

gauge* store_t::entries_instance(std::string name) {
  return &entries_family()->Add({{"name", std::move(name)}});
}

counter_family* store_t::processed_updates_family() {
  return &prometheus::BuildCounter()
            .Name("broker_store_processed_updates_total")
            .Help("Total number of processed data store updates.")
            .Register(*reg_);
}

counter* store_t::processed_updates_instance(std::string name) {
  return &processed_updates_family()->Add({{"name", std::move(name)}});
}

gauge_family* store_t::unacknowledged_updates_family() {
  return &prometheus::BuildGauge()
            .Name("broker_store_unacknowledged_updates")
            .Help("Number of unacknowledged data store updates.")
            .Register(*reg_);
}

gauge* store_t::unacknowledged_updates_instance(std::string name) {
  return &unacknowledged_updates_family()->Add({{"name", std::move(name)}});
}

} // namespace broker::internal
