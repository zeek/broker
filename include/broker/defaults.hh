#pragma once

#include "caf/string_view.hpp"
#include "caf/timespan.hpp"

// This header contains hard-coded default values for various Broker options.

namespace broker::defaults {

extern const caf::string_view recording_directory;

extern const size_t output_generator_file_cap;

constexpr uint16_t ttl = 20;

constexpr size_t max_pending_inputs_per_source = 512;

} // namespace broker::defaults

namespace broker::defaults::store {

extern const caf::timespan tick_interval;

} // namespace broker::defaults::store
