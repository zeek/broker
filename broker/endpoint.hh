#ifndef BROKER_ENDPOINT_HH
#define BROKER_ENDPOINT_HH

#include <cstdint>
#include <string>
#include <vector>

#include <caf/actor.hpp>

#include "broker/backend.hh"
#include "broker/backend_options.hh"
#include "broker/endpoint_info.hh"
#include "broker/expected.hh"
#include "broker/frontend.hh"
#include "broker/fwd.hh"
#include "broker/message.hh"
#include "broker/network_info.hh"
#include "broker/peer_info.hh"
#include "broker/status.hh"
#include "broker/store.hh"
#include "broker/topic.hh"

#include "broker/detail/operators.hh"

namespace broker {

/// The main publish/subscribe abstraction. Endpoints can *peer* which each
/// other to exchange messages. When publishing a message though an endpoint,
/// all peers with matching subscriptions receive the message.
class endpoint {
public:
  using value_type = std::pair<topic, data>;

  endpoint(context& ctx);

  endpoint(endpoint&&) = default;

  endpoint() = delete;
  endpoint(const endpoint&) = delete;
  endpoint& operator=(const endpoint&) = delete;

  /// @returns Information about this endpoint.
  endpoint_info info() const;

  // --- peer management -------------------------------------------------------

  /// Listens at a specific port to accept remote peers.
  /// @param address The interface to listen at. If empty, listen on all
  ///                local interfaces.
  /// @param port The port to listen locally. If 0, the endpoint selects the
  ///             next available free port from the OS
  /// @returns The port the endpoint bound to or 0 on failure.
  uint16_t listen(const std::string& address = {}, uint16_t port = 0);

  /// Initiates a peering with a remote endpoint. Thi
  /// @param address The IP address of the remote endpoint.
  /// @param port The TCP port of the remote endpoint.
  /// @param retry If non-zero, seconds after which to retry if connection
  ///        cannot be established, or breaks.
  /// @note The function returns immediately. The endpoint receives a status
  ///       message indicating the result of the peering operation.
  void peer(const std::string& address, uint16_t port, timeout::seconds retry = timeout::seconds(10));

  void unpeer(const std::string& address, uint16_t port);

  /// Retrieves a list of all known peers.
  /// @returns A pointer to the list
  std::vector<peer_info> peers() const;

  // --- publishing ------------------------------------------------------------

  /// Publishes a message.
  /// @param t The topic of the message.
  /// @param d The message data.
  void publish(topic t, data d);

  /// Publishes a message to a specific peer endpoint only.
  /// @param dst The destination endpoint.
  /// @param t The topic of the message.
  /// @param d The message data.
  void publish(const endpoint_info& dst, topic t, data d);

  /// Publishes a message with a custom message type.
  /// @param t The topic of the message.
  /// @param ty The custom type.
  /// @param d The message data.
  void publish(topic t, message_type ty, data d);

  /// Publishes a message to a specific peer endpoint only.
  /// @param dst The destination endpoint.
  /// @param t The topic of the message.
  /// @param ty The custom type.
  /// @param d The message data.
  void publish(const endpoint_info& dst, topic t, message_type ty, data d);

  /// Publishes a message.
  /// @param msg The message to publish.
  void publish(const message& msg);

  /// Publishes a message as vector.
  /// @param t The topic of the messages.
  /// @param xs The contents of the messages.
  void publish(topic t, std::initializer_list<data> xs);

  // Publishes all messages in `xs`.
  void publish(std::vector<value_type> xs);

  // --- subscribing -----------------------------------------------------------

  /// Returns a subscriber connected to this endpoint for the topics `ts`.
  subscriber make_subscriber(std::vector<topic> ts);

  // --- data stores -----------------------------------------------------------

  /// Attaches and/or creates a *master* data store with a globally unique name.
  /// @param name The name of the master.
  /// @param opts The options controlling backend construction.
  /// @returns A handle to the frontend representing the master or an error if
  ///          a master with *name* exists already.
  template <frontend F, backend B>
  auto attach(std::string name, backend_options opts = backend_options{})
  -> detail::enable_if_t<F == master, expected<store>> {
    return attach_master(std::move(name), B, std::move(opts));
  }

  /// Attaches and/or creates a *master* data store with a globally unique name.
  /// @param name The name of the master.
  /// @param type The backend type.
  /// @param opts The options controlling backend construction.
  /// @returns A handle to the frontend representing the master or an error if
  ///          a master with *name* exists already.
  template <frontend F>
  auto attach(std::string name, backend type,
              backend_options opts = backend_options{})
  -> detail::enable_if_t<F == master, expected<store>> {
    switch (type) {
      case memory:
        return attach<master, memory>(std::move(name), std::move(opts));
      case sqlite:
        return attach<master, sqlite>(std::move(name), std::move(opts));
      case rocksdb:
        return attach<master, rocksdb>(std::move(name), std::move(opts));
    }
  }

  /// Attaches and/or creates a *clone* data store to an existing master.
  /// @param name The name of the clone.
  /// @returns A handle to the frontend representing the clone, or an error if
  ///          a master *name* could not be found.
  template <frontend F>
  auto attach(std::string name)
  -> detail::enable_if_t<F == clone, expected<store>> {
    return attach_clone(std::move(name));
  }

  const caf::actor& core() const;

protected:
  template <class T>
  void add_to_vector(vector& v, T&& x) {
    v.emplace_back(std::forward<T>(x));
  }

  template <class T, class... Ts>
  void add_to_vector(vector& v, T&& x, Ts&&... xs) {
    add_to_vector(v, std::forward<T>(x));
    add_to_vector(v, std::forward<Ts>(xs)...);
  }

  void init_core(caf::actor core);

  caf::actor subscriber_;

private:
  expected<store> attach_master(std::string name, backend type,
                              backend_options opts);

  expected<store> attach_clone(std::string name);

  context& ctx_;
};

} // namespace broker

#endif // BROKER_ENDPOINT_HH
