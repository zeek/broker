#include "broker/defaults.hh"

#include <chrono>
#include <limits>

using namespace std::chrono_literals;

namespace broker::defaults {

const caf::string_view recording_directory = "";

const size_t output_generator_file_cap = std::numeric_limits<size_t>::max();

} // namespace broker::defaults

namespace broker::defaults::path_blacklist {

const caf::timespan aging_interval = 1s;

const caf::timespan max_age = 5min;

} // namespace broker::defaults::path_blacklist
