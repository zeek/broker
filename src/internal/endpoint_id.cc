#include "broker/internal/endpoint_id.hh"

#include "broker/internal/native.hh"

#include <stdexcept>

namespace broker::internal {

/// @throws std::invalid_argument if @p str is not properly formatted.
endpoint_id endpoint_id_from_string(std::string_view str) {
  caf::node_id res;
  if (auto err = caf::parse(str, res))
    throw std::invalid_argument("endpoint_id_from_string");
  return internal::facade(res);
}

} // namespace broker::internal
