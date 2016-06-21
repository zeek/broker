#ifndef BROKER_ENDPOINT_INFO_HH
#define BROKER_ENDPOINT_INFO_HH

#include <caf/node_id.hpp>

#include "broker/fwd.hh"
#include "broker/network_info.hh"
#include "broker/optional.hh"

namespace broker {

using caf::node_id;

/// Information about an endpoint.
/// @relates endpoint
struct endpoint_info {
  node_id node;                   ///< A unique context ID per machine/process.
  endpoint_id id;                 ///< A unique endpoint ID within a node.
  optional<network_info> network; ///< Optional network-level information.
};

template <class Processor>
void serialize(Processor& proc, endpoint_info& info) {
  proc & info.node;
  proc & info.id;
  proc & info.network;
}

} // namespace broker

#endif // BROKER_ENDPOINT_INFO_HH
