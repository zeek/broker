#ifndef BROKER_ENDPOINT_UID_HH
#define BROKER_ENDPOINT_UID_HH

#include <caf/node_id.hpp>

#include "broker/network_info.hh"
#include "broker/optional.hh"

namespace broker {

/// A per-machine, per-process unique identifier.
/// @relates endpoint_uid
using node_id = caf::node_id;

/// An endpoint identifier that is unique within a given [node](node_id).
/// @relates endpoint_uid
using endpoint_id = caf::actor_id;

/// A globally unique endpoint identifier. It consists of a node ID to
/// uniquely represent the combination of machine and OS process ID, and an
/// endpoint ID to uniquely represent an endpoint within a node.
/// @relates endpoint
struct endpoint_uid {
  /// Default-constructs an endpoint UID.
  endpoint_uid() = default;

  /// Constructs an endpoint UID from a node ID, endpoint ID, and optional
  /// network-level information.
  endpoint_uid(node_id nid, endpoint_id eid, optional<network_info> ni = {});

  node_id node;                   ///< The unique node ID.
  endpoint_id endpoint;           ///< The endpoint ID within the node.
  optional<network_info> network; ///< Optional network-level information.
};

/// @relates endpoint_uid
bool operator==(const endpoint_uid& x, const endpoint_uid& y);

/// @relates endpoint_uid
bool operator!=(const endpoint_uid& x, const endpoint_uid& y);

} // namespace broker

#endif // BROKER_ENDPOINT_UID_HH
