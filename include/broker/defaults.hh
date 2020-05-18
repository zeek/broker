#pragma once

#include "caf/string_view.hpp"
#include "caf/timespan.hpp"

// This header contains hard-coded default values for various Broker options.

namespace broker {
namespace defaults {

extern const caf::string_view recording_directory;

extern const size_t output_generator_file_cap;

extern const caf::timespan path_blacklist_aging_interval;

extern const caf::timespan path_blacklist_max_age;

} // namespace defaults
} // namespace broker
