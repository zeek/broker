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
  endpoint_info(endpoint_info&&) = default;
  endpoint_info(const endpoint_info&) = default;
  endpoint_info& operator=(endpoint_info&&) = default;
  endpoint_info& operator=(const endpoint_info&) = default;

  endpoint_info() : type("invalid") {
    // nop
  }

  explicit endpoint_info(endpoint_id id) : node(id), type("native") {
    // nop
  }

  endpoint_info(endpoint_id id, std::nullopt_t) : endpoint_info(id) {
    // nop
  }

  endpoint_info(endpoint_id id, network_info net)
    : node(id), network(std::move(net)), type("native") {
    // nop
  }

  endpoint_info(endpoint_id id, std::nullopt_t, std::string ep_type)
    : node(id), type(std::move(ep_type)) {
    // nop
  }

  endpoint_info(endpoint_id id, network_info net, std::string ep_type)
    : node(id), network(std::move(net)), type(std::move(ep_type)) {
    // nop
  }

  /// Uniquely identifies an endpoint in the network.
  endpoint_id node;

  /// Network-level information if available.
  std::optional<network_info> network;

  /// Denotes the type of an endpoint, e.g., "native" or "web-socket".
  std::string type;
};

/// @relates endpoint_info
inline bool operator==(const endpoint_info& x, const endpoint_info& y) {
  return x.node == y.node && x.network == y.network;
}

/// @relates endpoint_info
inline bool operator!=(const endpoint_info& x, const endpoint_info& y) {
  return !(x == y);
}

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

/// @relates endpoint_info
void convert(const endpoint_info& src, std::string& dst);

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

} // namespace broker
