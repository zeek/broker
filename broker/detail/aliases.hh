#include <string>
#include <utility>
#include <vector>

#include "caf/stream.hpp"

#include "broker/data.hh"
#include "broker/topic.hh"

namespace broker {
namespace detail {

using key_type = topic;

using value_type = data;

using filter_type = std::vector<key_type>;

using element_type = std::pair<key_type, value_type>;

using stream_type = caf::stream<element_type>;

} // namespace detail
} // namespace broker
