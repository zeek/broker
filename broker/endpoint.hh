#ifndef BROKER_ENDPOINT_HH
#define BROKER_ENDPOINT_HH

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>

#include "broker/backend.hh"
#include "broker/backend_options.hh"
#include "broker/configuration.hh"
#include "broker/endpoint_info.hh"
#include "broker/event_subscriber.hh"
#include "broker/expected.hh"
#include "broker/frontend.hh"
#include "broker/fwd.hh"
#include "broker/network_info.hh"
#include "broker/peer_info.hh"
#include "broker/status.hh"
#include "broker/store.hh"
#include "broker/topic.hh"

#include "broker/detail/filter_type.hh"
#include "broker/detail/operators.hh"

namespace broker {

/// The main publish/subscribe abstraction. Endpoints can *peer* which each
/// other to exchange messages. When publishing a message though an endpoint,
/// all peers with matching subscriptions receive the message.
class endpoint {
public:
  // --- member types ----------------------------------------------------------

  using value_type = std::pair<topic, data>;

  using stream_type = caf::stream<value_type>;

  using actor_init_fun = std::function<void (caf::event_based_actor*)>;

  // --- construction and destruction ------------------------------------------

  endpoint(configuration config = {});

  endpoint(endpoint&&) = delete;
  endpoint(const endpoint&) = delete;
  endpoint& operator=(endpoint&&) = delete;
  endpoint& operator=(const endpoint&) = delete;

  /// Calls `shutdown`.
  ~endpoint();

  /// Shuts down all background activity and blocks until all local subscribers
  /// and publishers have terminated. *Must* be the very last function call on
  /// this object before destroying it.
  /// @warning *Destroys* the underlying actor system. Calling *any* member
  ///          function afterwards except `shutdown` and the destructor is
  ///          undefined behavior.
  void shutdown();

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

  /// Initiates a peering with a remote endpoint.
  /// @param address The IP address of the remote endpoint.
  /// @param port The TCP port of the remote endpoint.
  /// @param retry If non-zero, seconds after which to retry if connection
  ///        cannot be established, or breaks.
  /// @returns True if connection was successfulluy set up.
  /// @note The endpoint will also receive a status message indicating
  ///       success or failure.
  bool peer(const std::string& address, uint16_t port,
            timeout::seconds retry = timeout::seconds(10));

  /// Initiates a peering with a remote endpoint, without waiting
  /// for the operation to complete.
  /// @param address The IP address of the remote endpoint.
  /// @param port The TCP port of the remote endpoint.
  /// @param retry If non-zero, seconds after which to retry if connection
  ///        cannot be established, or breaks.
  /// @note The function returns immediately. The endpoint receives a status
  ///       message indicating the result of the peering operation.
  void peer_nosync(const std::string& address, uint16_t port,
            timeout::seconds retry = timeout::seconds(10));

  /// Shuts down a peering with a remote endpoint.
  /// @param address The IP address of the remote endpoint.
  /// @param port The TCP port of the remote endpoint.
  /// @returns True if connection was successfully torn down.
  /// @note The endpoint will also receive a status message
  ///       indicating sucess or failure.
  bool unpeer(const std::string& address, uint16_t port);

  /// Shuts down a peering with a remote endpoint, without waiting for
  /// for the operation to complete.
  /// @param address The IP address of the remote endpoint.
  /// @param port The TCP port of the remote endpoint.
  /// @returns True if connection was successfully torn down.
  /// @note The endpoint will also receive a status message
  ///       indicating sucess or failure.
  void unpeer_nosync(const std::string& address, uint16_t port);

  /// Retrieves a list of all known peers.
  /// @returns A pointer to the list
  std::vector<peer_info> peers() const;

  /// Retrieves a list of topics that peers have subscribed to on this endpoint.
  std::vector<topic> peer_subscriptions() const;

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

  /// Publishes a message as vector.
  /// @param t The topic of the messages.
  /// @param xs The contents of the messages.
  void publish(topic t, std::initializer_list<data> xs);

  // Publishes all messages in `xs`.
  void publish(std::vector<value_type> xs);

  publisher make_publisher(topic ts);

  /// Starts a background worker from the given set of functions that publishes
  /// a series of messages. The worker will run in the background, but `init`
  /// is guaranteed to be called before the function returns.
  template <class Init, class GetNext, class AtEnd, class ResultHandler>
  caf::actor publish_all(Init init, GetNext f, AtEnd pred, ResultHandler rf) {
    std::mutex mx;
    std::condition_variable cv;
    auto res = make_actor([=,&mx,&cv](caf::event_based_actor* self) {
      self->make_source(
        core(),
        init,
        f,
        pred,
        rf 
      );
      std::unique_lock<std::mutex> guard{mx};
      cv.notify_one();
    });
    std::unique_lock<std::mutex> guard{mx};
    cv.wait(guard);
    return res;
  }

  /// Identical to ::publish_all, but does not guarantee that `init` is called
  /// before the function returns.
  template <class Init, class GetNext, class AtEnd, class ResultHandler>
  caf::actor publish_all_nosync(Init init, GetNext f, AtEnd pred,
                                ResultHandler rf) {
    return make_actor([=](caf::event_based_actor* self) {
      self->make_source(
        core(),
        init,
        f,
        pred,
        rf 
      );
    });
  }

  // --- subscribing events ----------------------------------------------------

  /// Returns a subscriber connected to this endpoint for receiving error and
  /// (optionally) status events.
  event_subscriber make_event_subscriber(bool receive_statuses = false);

  // --- subscribing data ------------------------------------------------------

  /// Returns a subscriber connected to this endpoint for the topics `ts`.
  subscriber make_subscriber(std::vector<topic> ts, long max_qsize = 20);

  /// Starts a background worker from the given set of function that consumes
  /// incoming messages. The worker will run in the background, but `init` is
  /// guaranteed to be called before the function returns.
  template <class Init, class HandleMessage, class Cleanup>
  caf::actor subscribe(std::vector<topic> topics, Init init, HandleMessage f,
                       Cleanup cleanup) {
    std::mutex mx;
    std::condition_variable cv;
    auto res = make_actor([=,&mx,&cv](caf::event_based_actor* self) {
      self->send(self * core(), atom::join::value, std::move(topics));
      self->become(
        [=](const stream_type& in) {
          self->make_sink(in, init, f, cleanup);
          self->unbecome();
        }
      );
      std::unique_lock<std::mutex> guard{mx};
      cv.notify_one();
    });
    std::unique_lock<std::mutex> guard{mx};
    cv.wait(guard);
    return res;
  }

  /// Identical to ::subscribe, but does not guarantee that `init` is called
  /// before the function returns.
  template <class Init, class HandleMessage, class Cleanup>
  caf::actor subscribe_nosync(std::vector<topic> topics, Init init,
                              HandleMessage f, Cleanup cleanup) {
    return make_actor([=](caf::event_based_actor* self) {
      self->send(self * core(), atom::join::value, std::move(topics));
      self->become(
        [=](const stream_type& in) {
          self->make_sink(in, init, f, cleanup);
          self->unbecome();
        }
      );
    });
  }

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
  throw std::domain_error("unknown backend type");
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

  /// Queries whether the endpoint waits for masters and slaves on shutdown.
  inline bool await_stores_on_shutdown() const {
    return await_stores_on_shutdown_;
  }

  /// Sets whether the endpoint waits for masters and slaves on shutdown.
  inline void await_stores_on_shutdown(bool x) {
    await_stores_on_shutdown_ = x;
  }

  // --- access to CAF state ---------------------------------------------------

  inline caf::actor_system& system() {
    return system_;
  }

  inline const caf::actor& core() const {
    return core_;
  }

protected:
  caf::actor subscriber_;

private:
  caf::actor make_actor(actor_init_fun f);

  expected<store> attach_master(std::string name, backend type,
                                backend_options opts);

  expected<store> attach_clone(std::string name);

  configuration config_;
  union {
    mutable caf::actor_system system_;
  };
  caf::actor core_;
  bool await_stores_on_shutdown_;
  std::vector<caf::actor> children_;
  bool destroyed_;
};

} // namespace broker

#endif // BROKER_ENDPOINT_HH
