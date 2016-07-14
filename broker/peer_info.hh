#ifndef BROKER_PEER_INFO_HH
#define BROKER_PEER_INFO_HH

#include "broker/endpoint_info.hh"
#include "broker/peer_flags.hh"
#include "broker/peer_status.hh"

namespace broker {

/// Information about a peer of an endpoint.
/// @relates endpoint
struct peer_info {
  endpoint_info peer;   ///< Information about the peer.
  peer_flags flags;     ///< Details about peering relationship.
  peer_status status;   ///< The current peering status.
};

template <class Processor>
void serialize(Processor& proc, peer_info& pi) {
  proc & pi.peer;
  proc & pi.flags;
  proc & pi.status;
}

} // namespace broker

#endif // BROKER_PEER_INFO_HH
