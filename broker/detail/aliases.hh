#include <string>
#include <utility>
#include <vector>

#include "caf/stream.hpp"

namespace broker {
namespace detail {

using key_type = std::string;

using value_type = int;

using filter_type = std::vector<key_type>;

using element_type = std::pair<key_type, value_type>;

using stream_type = caf::stream<element_type>;

} // namespace detail
} // namespace broker
