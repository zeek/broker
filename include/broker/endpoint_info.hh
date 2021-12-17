#pragma once

#include "broker/convert.hh"
#include "broker/endpoint_id.hh"
#include "broker/fwd.hh"
#include "broker/network_info.hh"

#include <optional>
#include <vector>

namespace broker {

/// Information about an endpoint.
/// @relates endpoint
struct endpoint_info {
  endpoint_id node; ///< A unique context ID per machine/process.
  std::optional<network_info> network; ///< Optional network-level information.
};

/// @relates endpoint_info
template <class Inspector>
bool inspect(Inspector& f, endpoint_info& x) {
  return f.object(x)
    .pretty_name("endpoint_info")
    .fields(f.field("node", x.node), f.field("network", x.network));
}

/// @relates endpoint_info
bool convertible_to_endpoint_info(const data& src);

/// @relates endpoint_info
bool convertible_to_endpoint_info(const std::vector<data>& src);

/// @relates endpoint_info
bool convert(const data& src, endpoint_info& dst);

/// @relates endpoint_info
bool convert(const endpoint_info& src, data& dst);

// Enable `can_convert` for `endpoint_info`.
template <>
struct can_convert_predicate<endpoint_info> {
  static bool check(const data& src) {
    return convertible_to_endpoint_info(src);
  }

  static bool check(const std::vector<data>& src) {
    return convertible_to_endpoint_info(src);
  }
};

/// @relates endpoint_info
std::string to_string(const endpoint_info& x);

} // namespace broker
