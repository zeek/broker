#pragma once

#include "broker/event.hh"
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

  /// Called by Broker to notify the observer about a new client connection.
  /// Clients are non-native endpoints, e.g. via WebSockets, that don't support
  /// the full peering machinery.
  virtual void on_client_connect(const endpoint_id& client,
                                 const network_info& info);

  /// Called by Broker to notify the observer about a new outgoing message to a
  /// client. This function is called if the message enters the client's buffer.
  virtual void on_client_buffer_push(const endpoint_id& client,
                                     const data_message& msg);

  /// Called by Broker to notify the observer about a new outgoing message to a
  /// client. This function is called if the message leaves the client's buffer.
  virtual void on_client_buffer_pull(const endpoint_id& client,
                                     const data_message& msg);

  /// Called by Broker to notify the observer about a discarded client
  /// connection.
  /// @param client The ID of the client that disconnected.
  /// @param reason The reason for the disconnect or an empty error on a
  ///               graceful connection shutdown.
  virtual void on_client_disconnect(const endpoint_id& client,
                                    const error& reason);

  /// Called by Broker to notify the observer about a new event.
  /// @param what The event that Broker has emitted.
  /// @note This member function is called from multiple threads and thus must
  ///       be thread-safe.
  virtual void observe(event_ptr what) = 0;

  /// Returns true if the observer is interested in events of the given severity
  /// and component type. Returning false will cause Broker to not generate
  /// filtered events.
  virtual bool accepts(event::severity_level severity,
                       event::component_type component) const = 0;
};

/// A smart pointer holding an ::event_observer.
using event_observer_ptr = std::shared_ptr<event_observer>;

} // namespace broker
