#ifndef BROKER_ENDPOINT_HH
#define BROKER_ENDPOINT_HH

#include <cstdint>
#include <string>
#include <vector>

#include <caf/actor.hpp>
#include <caf/scoped_actor.hpp>

#include "broker/message.hh"
#include "broker/topic.hh"

namespace broker {

class context;
class endpoint;
class blocking_endpoint;
class nonblocking_endpoint;

/// A peer of an endpoint.
class peer {
  friend endpoint; // construction
public:
  peer() = default;

  std::string address() const;
  uint16_t port() const;

private:
  peer(caf::actor_addr addr);

  caf::actor_addr addr_;
};

/// The main messaging abstraction.
class endpoint {
  friend context;

public:
  endpoint() = default;

  endpoint(const blocking_endpoint&);
  endpoint(const nonblocking_endpoint&);

  endpoint& operator=(const blocking_endpoint& other);
  endpoint& operator=(const nonblocking_endpoint& other);

  explicit operator bool() const noexcept;
  bool operator!() const noexcept;

  /// Initiates a peering with another endpoint.
  /// @param other The endpoint to peer with.
  bool peer(const endpoint& other);

  /// Initiates a peering with a remote endpoint.
  /// @param address The IP address of the remote endpoint.
  /// @param port The TCP port of the remote endpoint.
  bool peer(const std::string& address, uint16_t port);

  /// Unpeers from another endpoint.
  /// @param other The endpoint to unpeer from.
  bool unpeer(const endpoint& other);

  /// Retrieves a list of all known peers.
  /// @returns A pointer to the list
  std::vector<broker::peer> peers() const;

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
  caf::actor core_;
};

/// An endpoint with a synchronous (blocking) API to retrieve messages.
class blocking_endpoint : public endpoint {
  friend context; // construction

public:
  /// Consumes the next message in the mailbox or blocks until one arrives.
  /// @returns The next message in the mailbox.
  message receive();

  /// Consumes one message that matches the given handler.
  template <class T, class... Ts>
  void receive(T&& x, Ts&&... xs) {
    (*subscriber_)->receive(std::forward<T>(x), std::forward<Ts>(xs)...);
  }

  /// Checks whether the mailbox of the endpoint is empty.
  /// @returns `true` if the endpoint has no messages in its mailbox.
  bool empty() const;

  /// Retrieves the number of queued messages in the mailbox.
  /// @returns The mailbox size.
  //size_t mailbox_size() const;

  /// Retrieves a descriptor that indicates whether a message can be retrieved
  /// without blocking.
  /// @returns A descriptor which is ready when `mailbox_size() > 0`.
  int descriptor();

private:
  blocking_endpoint(caf::actor_system& sys);

  std::shared_ptr<caf::scoped_actor> subscriber_;
};

/// An endpoint with an asynchronous (nonblocking) API to retrieve messages.
class nonblocking_endpoint : public endpoint {
  friend context; // construction

public:
  // Nothing to see here; full behavior specified upon construction.

private:
  nonblocking_endpoint(caf::actor_system& sys, caf::actor subscriber);
};

} // namespace broker

#endif // BROKER_ENDPOINT_HH
