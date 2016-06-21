#ifndef BROKER_ENDPOINT_HH
#define BROKER_ENDPOINT_HH

#include <cstdint>
#include <string>
#include <vector>

#include <caf/actor.hpp>

#include "broker/detail/operators.hh"
#include "broker/detail/scoped_flare_actor.hh"

#include "broker/endpoint_info.hh"
#include "broker/network_info.hh"
#include "broker/message.hh"
#include "broker/optional.hh"
#include "broker/peer_flags.hh"
#include "broker/topic.hh"

namespace broker {

/// Describes the possible states of a peer. A local peer begins in state
/// `initialized` and transitions directly to `peered`. A remote peer
/// begins in `initialized` and then through states `connecting`, `connected`,
/// and then `peered`.
enum class peer_status {
  initialized,    ///< The peering process has been initiated.
  connecting,     ///< Connection establishment is in progress.
  connected,      ///< Connection has been established, peering pending.
  peered,         ///< Successfully peering.
  disconnected,   ///< Connection to remote peer lost.
  reconnecting,   ///< Reconnecting after a lost connection.
};

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

/// The main publish/subscribe abstraction. Endpoints can *peer* which each
/// other to exchange messages. When publishing a message though an endpoint,
/// all peers with matching subscriptions receive the message.
class endpoint {
  friend context;

public:
  endpoint(const blocking_endpoint&);
  endpoint(const nonblocking_endpoint&);

  endpoint& operator=(const blocking_endpoint& other);
  endpoint& operator=(const nonblocking_endpoint& other);

  /// @returns Information about this endpoint.
  endpoint_info info() const;

  /// Listens at a specific port to accept remote peers.
  /// @param address The interface to listen at. If empty, listen on all
  ///                local interfaces.
  /// @param port The port to listen locally. If 0, the endpoint selects the
  ///             next available free port from the OS
  /// @returns The port the endpoint bound to or 0 on failure.
  uint16_t listen(const std::string& address = {}, uint16_t port = 0);

  /// Initiates a peering with another endpoint.
  /// @param other The endpoint to peer with.
  /// @note The function returns immediately. The endpoint receives a status
  ///       message indicating the result of the peering operation.
  void peer(const endpoint& other);

  /// Initiates a peering with a remote endpoint. Thi
  /// @param address The IP address of the remote endpoint.
  /// @param port The TCP port of the remote endpoint.
  /// @note The function returns immediately. The endpoint receives a status
  ///       message indicating the result of the peering operation.
  void peer(const std::string& address, uint16_t port);

  /// Unpeers from another endpoint.
  /// @param other The endpoint to unpeer from.
  /// @note The function returns immediately. The endpoint receives a status
  ///       message indicating the result of the peering operation.
  void unpeer(const endpoint& other);

  void unpeer(const std::string& address, uint16_t port);

  /// Retrieves a list of all known peers.
  /// @returns A pointer to the list
  std::vector<peer_info> peers() const;

  /// Publishes a message.
  /// @param t The topic of the message.
  /// @param msg The message.
  void publish(topic t, message msg);

  /// Publishes a message.
  /// @param t The topic of the message.
  /// @param xs The message contents.
  template <class... Ts>
  void publish(topic t, Ts&&... xs) {
    publish(std::move(t), make_message(std::forward<Ts>(xs)...));
  }

  /// Subscribes to a topic.
  void subscribe(topic t);

  /// Unsubscribes from a topic.
  /// @param t The topic to unsubscribe from.
  void unsubscribe(topic t);

protected:
  endpoint() = default;

  const caf::actor& core() const;

  std::shared_ptr<caf::actor> core_;
};

/// An endpoint with a synchronous (blocking) messaging API.
class blocking_endpoint : public endpoint {
  friend context; // construction

public:
  /// Consumes the next message in the mailbox or blocks until one arrives.
  /// @returns The next message in the mailbox.
  message receive();

  /// Consumes one message that matches the given handler.
  template <class T, class... Ts>
  void receive(T&& x, Ts&&... xs) {
    caf::behavior bhvr{std::forward<T>(x), std::forward<Ts>(xs)...};
    subscriber_->dequeue(bhvr);
  }

  /// Access the endpoint's mailbox, which provides the following
  /// introspection functions:
  ///
  /// - `int descriptor()`: Retrieves a descriptor that indicates whether a
  ///   message can be received without blocking.
  ///
  /// - `bool empty()`: Checks whether the endpoint's message mailbox is empty.
  ///
  /// - `size_t count(size_t max)`: Counts the number of messages in the
  ///   mailbox in time that is a linear function of the mailbox size. The
  ///   parameter `max` allows for specifying an upper bound when to stop
  ///   counting.
  ///
  /// @returns A proxy object to introspect the endpoint's mailbox.
  detail::mailbox mailbox();

private:
  blocking_endpoint(caf::actor_system& sys);

  std::shared_ptr<detail::scoped_flare_actor> subscriber_;
};

/// An endpoint with an asynchronous (nonblocking) messaging API.
class nonblocking_endpoint : public endpoint {
  friend context; // construction

public:
  // Nothing to see here, please move along. The behavior of a nonblocking
  // endpoint is fully specified when spawned via a context instance.

private:
  nonblocking_endpoint(caf::actor_system& sys, caf::actor subscriber);
};

} // namespace broker

#endif // BROKER_ENDPOINT_HH
