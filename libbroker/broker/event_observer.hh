#pragma once

#include "broker/fwd.hh"

#include <memory>

namespace broker {

/// An interface for observing internal events in Broker.
class event_observer {
public:
  virtual ~event_observer();

  /// Called by Broker to notify the observer about a new peer connection.
  virtual void on_peer_connect(const endpoint_id& peer,
                               const network_info& info);

  /// Called by Broker to notify the observer about a new outgoing message to a
  /// peer. This function is called if the message enters the peer's buffer.
  virtual void on_peer_buffer_push(const endpoint_id& peer,
                                   const node_message& msg);

  /// Called by Broker to notify the observer about a new outgoing message to a
  /// peer. This function is called if the message leaves the peer's buffer.
  virtual void on_peer_buffer_pull(const endpoint_id& peer,
                                   const node_message& msg);

  /// Called by Broker to notify the observer about a discarded peer connection.
  /// @param peer The ID of the peer that disconnected.
  /// @param reason The reason for the disconnect or an empty error on a
  ///               graceful connection shutdown.
  virtual void on_peer_disconnect(const endpoint_id& peer, const error& reason);
};

/// A smart pointer holding an ::event_observer.
using event_observer_ptr = std::shared_ptr<event_observer>;

} // namespace broker
