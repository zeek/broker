#pragma once

#include "broker/fwd.hh"

#include <caf/async/fwd.hpp>
#include <caf/fwd.hpp>
#include <caf/net/fwd.hpp>

namespace broker::internal {

/// Represents a pending connection to a peer. The handshake has been completed
/// but the core actor needs to provide the asynchronous buffers for exchanging
/// messages to the peer.
class pending_connection {
public:
  virtual ~pending_connection();

  /// Acknowledges and runs the connection in the background.
  /// @param sys the actor system with the network manager.
  /// @param pull The resource where the connection pulls data from.
  /// @param push The resource where the connection pushes data to.
  virtual caf::error run(caf::actor_system& sys,
                         caf::async::consumer_resource<node_message> pull,
                         caf::async::producer_resource<node_message> push) = 0;
};

/// @relates pending_connection
using pending_connection_ptr = std::shared_ptr<pending_connection>;

} // namespace broker::internal
