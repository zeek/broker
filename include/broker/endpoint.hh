#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "broker/backend.hh"
#include "broker/backend_options.hh"
#include "broker/detail/sink_driver.hh"
#include "broker/detail/source_driver.hh"
#include "broker/endpoint_id.hh"
#include "broker/endpoint_info.hh"
#include "broker/expected.hh"
#include "broker/frontend.hh"
#include "broker/fwd.hh"
#include "broker/message.hh"
#include "broker/network_info.hh"
#include "broker/peer_info.hh"
#include "broker/status.hh"
#include "broker/status_subscriber.hh"
#include "broker/store.hh"
#include "broker/time.hh"
#include "broker/topic.hh"
#include "broker/worker.hh"

namespace broker::internal {

struct endpoint_access;
struct endpoint_context;

} // namespace broker::internal

namespace broker {

/// The main publish/subscribe abstraction. Endpoints can *peer* which each
/// other to exchange messages. When publishing a message though an endpoint,
/// all peers with matching subscriptions receive the message.
class endpoint {
public:
  // --- friends ---------------------------------------------------------------

  friend struct internal::endpoint_access;

  // --- member types ----------------------------------------------------------

  /// Custom clock for either running in realtime mode or advancing time
  /// manually.
  class clock {
  public:
    // --- construction and destruction ----------------------------------------

    explicit clock(internal::endpoint_context* ctx);

    virtual ~clock();

    // -- accessors ------------------------------------------------------------

    virtual timestamp now() const noexcept = 0;

    virtual bool real_time() const noexcept = 0;

    // -- mutators -------------------------------------------------------------

    virtual void advance_time(timestamp t) = 0;

    virtual void send_later(worker dest, timespan after, void* msg) = 0;

  protected:
    /// Points to the host system.
    internal::endpoint_context* ctx_;
  };

  /// Utility class for configuring the metrics exporter.
  class metrics_exporter_t {
  public:
    explicit metrics_exporter_t(endpoint* parent) : parent_(parent) {
      // nop
    }

    metrics_exporter_t(const metrics_exporter_t&) noexcept = default;
    metrics_exporter_t& operator=(const metrics_exporter_t&) noexcept = default;

    /// Changes the frequency for publishing scraped metrics to the topic.
    /// Passing a zero-length interval has no effect.
    void set_interval(timespan new_interval);

    /// Sets a new target topic for the metrics. Passing an empty topic has no
    /// effect.
    void set_target(topic new_target);

    /// Sets a new ID for the metrics exporter. Passing an empty string has no
    /// effect.
    void set_id(std::string new_id);

    /// Sets a prefix selection for the metrics exporter. An empty vector means
    /// *all*.
    void set_prefixes(std::vector<std::string> new_prefixes);

  private:
    endpoint* parent_;
  };

  struct background_task {
    virtual ~background_task();
  };

  friend class metrics_exporter_t;

  // --- construction and destruction ------------------------------------------

  endpoint();

  explicit endpoint(configuration config);

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

  /// @returns a unique node id for this endpoint.
  endpoint_id node_id() const;

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

  /// Initiates a peering with a remote endpoint.
  /// @param info Bundles IP address, port, and retry interval for connecting to
  ///             the remote endpoint.
  /// @returns True if connection was successfulluy set up.
  /// @note The endpoint will also receive a status message indicating
  ///       success or failure.
  bool peer(const network_info& info) {
    return peer(info.address, info.port, info.retry);
  }

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

  // Publishes the messages `x`.
  void publish(data_message x);

  // Publishes all messages in `xs`.
  void publish(std::vector<data_message> xs);

  publisher make_publisher(topic ts);

  /// Starts a background worker from the given set of functions that publishes
  /// a series of messages. The worker will run in the background, but `init`
  /// is guaranteed to be called before the function returns.
  template <class Init, class Pull, class AtEnd>
  worker publish_all(Init init, Pull f, AtEnd pred) {
    using driver_t = detail::source_driver_impl<Init, Pull, AtEnd>;
    auto driver = std::make_shared<driver_t>(std::move(init), std::move(f),
                                             std::move(pred));
    return do_publish_all(std::move(driver), true);
  }

  /// Identical to ::publish_all, but does not guarantee that `init` is called
  /// before the function returns.
  template <class Init, class Pull, class AtEnd>
  worker publish_all_nosync(Init init, Pull f, AtEnd pred) {
    using driver_t = detail::source_driver_impl<Init, Pull, AtEnd>;
    auto driver = std::make_shared<driver_t>(std::move(init), std::move(f),
                                             std::move(pred));
    return do_publish_all(std::move(driver), false);
  }

  // --- subscribing events ----------------------------------------------------

  /// Returns a subscriber connected to this endpoint for receiving error and
  /// (optionally) status events.
  status_subscriber make_status_subscriber(bool receive_statuses = false);

  // --- forwarding events -----------------------------------------------------

  // Forward remote events for given topics even if no local subscriber.
  void forward(std::vector<topic> ts);

  // --- subscribing data ------------------------------------------------------

  /// Returns a subscriber connected to this endpoint for the topics `ts`.
  subscriber make_subscriber(std::vector<topic> ts, size_t max_qsize = 20u);

  /// Starts a background worker from the given set of function that consumes
  /// incoming messages. The worker will run in the background, but `init` is
  /// guaranteed to be called before the function returns.
  template <class Init, class HandleMessage, class Cleanup>
  worker subscribe(std::vector<topic> topics, Init init, HandleMessage f,
                   Cleanup cleanup) {
    using driver_t = detail::sink_driver_impl<Init, HandleMessage, Cleanup>;
    auto driver = std::make_shared<driver_t>(std::move(init), std::move(f),
                                             std::move(cleanup));
    return do_subscribe(std::move(topics), std::move(driver), true);
  }

  /// Identical to ::subscribe, but does not guarantee that `init` is called
  /// before the function returns.
  template <class Init, class HandleMessage, class Cleanup>
  worker subscribe_nosync(std::vector<topic> topics, Init init, HandleMessage f,
                          Cleanup cleanup) {
    using driver_t = detail::sink_driver_impl<Init, HandleMessage, Cleanup>;
    auto driver = std::make_shared<driver_t>(std::move(init), std::move(f),
                                             std::move(cleanup));
    return do_subscribe(std::move(topics), std::move(driver), false);
  }

  // --- data stores -----------------------------------------------------------

  /// Attaches and/or creates a *master* data store with a globally unique name.
  /// @param name The name of the master.
  /// @param type The type of backend to use.
  /// @param opts The options controlling backend construction.
  /// @returns A handle to the frontend representing the master or an error if
  ///          a master with *name* exists already.
  expected<store> attach_master(std::string name, backend type,
                                backend_options opts=backend_options());


  /// Attaches and/or creates a *clone* data store to an existing master.
  /// @param name The name of the clone.
  /// @param resync_interval The frequency at which the clone will attempt to
  ///                        reconnect/resynchronize with its master in the
  ///                        event that it becomes disconnected (in seconds).
  /// @param stale_interval The amount of time (seconds) after which a clone
  ///                       that is disconnected from its master will start
  ///                       to treat its local cache as stale.  In the stale
  ///                       state, it responds to queries with an error.  A
  ///                       negative value here means the local cache never
  ///                       goes stale.
  /// @param mutation_buffer_interval The maximum amount of time (seconds)
  ///                                 that a disconnected clone will buffer
  ///                                 data store mutation commands.  If the
  ///                                 clone reconnects before this time, it
  ///                                 will replay all stored commands.  Note
  ///                                 that this doesn't completely prevent
  ///                                 the loss of store updates: all mutation
  ///                                 messages are fire-and-forget and not
  ///                                 explicitly acknowledged by the master.
  ///                                 A negative/zero value here indicates to
  ///                                 never buffer commands.
  /// @returns A handle to the frontend representing the clone, or an error if
  ///          a master *name* could not be found.
  expected<store> attach_clone(std::string name, double resync_interval=10.0,
                               double stale_interval=300.0,
                               double mutation_buffer_interval=120.0);

  // --- messaging -------------------------------------------------------------

  void send_later(worker who, timespan after, void* msg) {
    clock_->send_later(std::move(who), after, msg);
  }

  /// Blocks the current thread until `who` terminates.
  void wait_for(worker who);

  // --- properties ------------------------------------------------------------

  /// Queries whether the endpoint waits for masters and slaves on shutdown.
  bool await_stores_on_shutdown() const {
    return await_stores_on_shutdown_;
  }

  /// Sets whether the endpoint waits for masters and slaves on shutdown.
  void await_stores_on_shutdown(bool x) {
    await_stores_on_shutdown_ = x;
  }

  /// Returns a configuration object for the metrics exporter.
  metrics_exporter_t metrics_exporter() {
    return metrics_exporter_t{this};
  }

  bool is_shutdown() const {
    return destroyed_;
  }

  bool use_real_time() const {
    return clock_->real_time();
  }

  timestamp now() const {
    return clock_->now();
  }

  void advance_time(timestamp t) {
    clock_->advance_time(t);
  }

  const worker& core() const {
    return core_;
  }

  broker_options options() const;

protected:
  worker subscriber_;

private:
  worker do_subscribe(std::vector<topic>&& topics,
                      std::shared_ptr<detail::sink_driver> driver,
                      bool block_until_initialized);

  worker do_publish_all(std::shared_ptr<detail::source_driver> driver,
                            bool block_until_initialized);

  template <class F>
  worker make_worker(F fn);

  std::shared_ptr<internal::endpoint_context> ctx_;
  worker core_;
  worker telemetry_exporter_;
  bool await_stores_on_shutdown_ = false;
  std::vector<worker> children_;
  bool destroyed_ = false;
  std::unique_ptr<clock> clock_;
  std::vector<std::unique_ptr<background_task>> background_tasks_;
};

} // namespace broker
