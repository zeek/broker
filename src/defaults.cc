#include "broker/defaults.hh"

#include <chrono>
#include <limits>

using namespace std::chrono_literals;

namespace broker::defaults {

const caf::string_view recording_directory = "";

const size_t output_generator_file_cap = std::numeric_limits<size_t>::max();

} // namespace broker::defaults

namespace broker::defaults::store {

const caf::timespan tick_interval = 50ms;

} // namespace broker::defaults::store
