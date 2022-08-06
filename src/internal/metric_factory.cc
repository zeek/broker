#include "broker/internal/metric_factory.hh"

#include <caf/actor_system.hpp>

namespace broker::internal {

// -- 'imports' to safe ourselves some typing ----------------------------------

using dbl_gauge = metric_factory::dbl_gauge;

using dbl_gauge_family = metric_factory::dbl_gauge_family;

using int_gauge = metric_factory::int_gauge;

using int_gauge_family = metric_factory::int_gauge_family;

using dbl_counter = metric_factory::dbl_counter;

using dbl_counter_family = metric_factory::dbl_counter_family;

using int_counter = metric_factory::int_counter;

using int_counter_family = metric_factory::int_counter_family;

using dbl_histogram = metric_factory::dbl_histogram;

using dbl_histogram_family = metric_factory::dbl_histogram_family;

using int_histogram = metric_factory::int_histogram;

using int_histogram_family = metric_factory::int_histogram_family;

// -- core metrics -------------------------------------------------------------

using core_t = metric_factory::core_t;

int_gauge_family* core_t::connections_family() {
  return reg_->gauge_family("broker", "connections", {"type"},
                            "Number of active network connections.");
}

core_t::connections_t core_t::connections_instances() {
  auto fm = connections_family();
  return {
    fm->get_or_add({{"type", "native"}}),
    fm->get_or_add({{"type", "web-socket"}}),
  };
}

int_counter_family* core_t::processed_messages_family() {
  return reg_->counter_family("broker", "processed-messages", {"type"},
                              "Total number of processed messages.", "1", true);
}

core_t::processed_messages_t core_t::processed_messages_instances() {
  auto fm = processed_messages_family();
  return {
    fm->get_or_add({{"type", "data"}}),
    fm->get_or_add({{"type", "command"}}),
    fm->get_or_add({{"type", "routing-update"}}),
    fm->get_or_add({{"type", "ping"}}),
    fm->get_or_add({{"type", "pong"}}),
  };
}

int_gauge_family* core_t::buffered_messages_family() {
  return reg_->gauge_family("broker", "buffered-messages", {"type"},
                            "Number of currently buffered messages.");
}

core_t::buffered_messages_t core_t::buffered_messages_instances() {
  auto fm = buffered_messages_family();
  return {
    fm->get_or_add({{"type", "data"}}),
    fm->get_or_add({{"type", "command"}}),
    fm->get_or_add({{"type", "routing-update"}}),
    fm->get_or_add({{"type", "ping"}}),
    fm->get_or_add({{"type", "pong"}}),
  };
}

// -- store metrics ------------------------------------------------------------

using store_t = metric_factory::store_t;

int_gauge_family* store_t::input_channels_family() {
  return reg_->gauge_family("broker", "store-input-channels", {"name"},
                            "Number of active input channels in a data store.");
}

int_gauge* store_t::input_channels_instance(std::string_view name) {
  return input_channels_family()->get_or_add({{"name", name}});
}

int_gauge_family* store_t::out_of_order_updates_family() {
  return reg_->gauge_family("broker", "store-input-channels", {"name"},
                            "Number of active input channels in a data store.");
}

int_gauge* store_t::out_of_order_updates_instance(std::string_view name) {
  return out_of_order_updates_family()->get_or_add({{"name", name}});
}

int_gauge_family* store_t::output_channels_family() {
  return reg_->gauge_family(
    "broker", "store-output-channels", {"name"},
    "Number of active output channels in a data store.");
}

int_gauge* store_t::output_channels_instance(std::string_view name) {
  return output_channels_family()->get_or_add({{"name", name}});
}

int_gauge_family* store_t::entries_family() {
  return reg_->gauge_family("broker", "store-entries", {"name"},
                            "Number of entries in the data store.");
}

int_gauge* store_t::entries_instance(std::string_view name) {
  return entries_family()->get_or_add({{"name", name}});
}

int_counter_family* store_t::processed_updates_family() {
  return reg_->counter_family("broker", "store-processed-updates", {"name"},
                              "Number of processed data store updates.", "1",
                              true);
}

int_counter* store_t::processed_updates_instance(std::string_view name) {
  return processed_updates_family()->get_or_add({{"name", name}});
}

int_gauge_family* store_t::unacknowledged_updates_family() {
  return reg_->gauge_family("broker", "store-unacknowledged-updates", {"name"},
                            "Number of unacknowledged data store updates.");
}

int_gauge* store_t::unacknowledged_updates_instance(std::string_view name) {
  return unacknowledged_updates_family()->get_or_add({{"name", name}});
}

// --- constructors ------------------------------------------------------------

metric_factory::metric_factory(caf::actor_system& sys) noexcept
  : metric_factory(sys.metrics()) {
  // nop
}

} // namespace broker::internal
