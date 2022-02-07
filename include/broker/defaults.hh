#pragma once

#include "broker/time.hh"

#include <limits>
#include <string_view>

// This header contains hard-coded default values for various Broker options.

namespace broker::defaults {

constexpr std::string_view recording_directory = "";

constexpr size_t output_generator_file_cap = std::numeric_limits<size_t>::max();

constexpr uint16_t ttl = 20;

constexpr size_t max_pending_inputs_per_source = 512;

} // namespace broker::defaults

namespace broker::defaults::store {

constexpr timespan tick_interval = std::chrono::milliseconds(50);

} // namespace broker::defaults::store

namespace broker::defaults::metrics {

constexpr timespan export_interval = std::chrono::seconds(1);

} // namespace broker::defaults::metrics::export
