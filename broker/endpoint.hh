#ifndef BROKER_ENDPOINT_HH
#define BROKER_ENDPOINT_HH

#include <cstdint>
#include <string>
#include <vector>

#include <caf/actor.hpp>

#include "broker/backend.hh"
#include "broker/endpoint_info.hh"
#include "broker/expected.hh"
#include "broker/frontend.hh"
#include "broker/fwd.hh"
#include "broker/message.hh"
#include "broker/network_info.hh"
#include "broker/peer_info.hh"
#include "broker/store.hh"
#include "broker/topic.hh"

#include "broker/detail/operators.hh"


namespace broker {

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

  // --- peer management -----------------------------------------------------

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

  // --- pub/sub API ---------------------------------------------------------

  /// Publishes a message.
  /// @param t The topic of the message.
  /// @param msg The message.
  void publish(topic t, message msg);

  /// Publishes a message.
  /// @param t The topic of the message.
  /// @param xs The message contents.
  template <class... Ts>
  void publish(topic t, Ts&&... xs) {
    publish(std::move(t), make_data_message(std::forward<Ts>(xs)...));
  }

  /// Subscribes to a topic.
  /// @param t The topic to subscribe to.
  void subscribe(topic t);

  /// Unsubscribes from a topic.
  /// @param t The topic to unsubscribe from.
  void unsubscribe(topic t);

  // --- data store API ------------------------------------------------------

  /// Attaches and/or creates a *master* data store with a globally unique name.
  /// @param name The name of the master.
  /// @returns A handle to the frontend representing the master.
  template <frontend F, backend B = memory>
  auto attach(std::string name)
  -> detail::enable_if_t<F == master, expected<store>> {
    return attach_master(std::move(name));
  }

  /// Attaches and/or creates a *clone* data store with a globally unique name.
  /// @param name The name of the clone.
  /// @returns A handle to the frontend representing the clone.
  template <frontend F>
  auto attach(std::string name)
  -> detail::enable_if_t<F == clone, expected<store>> {
    return attach_clone(std::move(name));
  }

protected:
  endpoint();

  void init_core(caf::actor core);

  const caf::actor& core() const;

  std::shared_ptr<caf::actor> core_;
  caf::actor subscriber_;

private:
  expected<store> attach_master(std::string name);
  expected<store> attach_clone(std::string name);
};

} // namespace broker

#endif // BROKER_ENDPOINT_HH
