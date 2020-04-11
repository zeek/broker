#include "broker/convert.hh"

#include <caf/node_id.hpp>

#include "broker/data.hh"

namespace broker::detail {

bool can_convert_data_to_node(const data& src) {
  if (auto str = get_if<std::string>(src)) {
    return caf::node_id::can_parse(*str);
  }
  return is<none>(src);
}

} // namespace broker::detail
