#include "broker/convert.hh"

#include "broker/data.hh"
#include "broker/endpoint_id.hh"

namespace broker::detail {

bool can_convert_data_to_node(const data& src) {
  if (const auto* str = get_if<std::string>(src)) {
    return endpoint_id::can_parse(*str);
  }
  return is<none>(src);
}

} // namespace broker::detail
