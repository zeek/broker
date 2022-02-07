#pragma once

#include "broker/endpoint_id.hh"

namespace broker::internal {

/// @throws std::invalid_argument if @p str is not properly formatted.
endpoint_id endpoint_id_from_string(std::string_view str);

} // namespace broker::internal
