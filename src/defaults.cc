#include "broker/defaults.hh"

#include <chrono>
#include <limits>

using namespace std::chrono_literals;

namespace broker {
namespace defaults {

const caf::string_view recording_directory = "";

const size_t output_generator_file_cap = std::numeric_limits<size_t>::max();

const caf::timespan path_blacklist_aging_interval = 1s;

const caf::timespan path_blacklist_max_age = 5min;

} // namespace defaults
} // namespace broker
