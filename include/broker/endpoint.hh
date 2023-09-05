#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "broker/backend.hh"
#include "broker/backend_options.hh"
#include "broker/configuration.hh"
#include "broker/defaults.hh"
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
#include "broker/shutdown_options.hh"
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

/// The main publish/subscribe abstraction. Endpoints can *peer* with each
/// other to exchange messages. When publishing a message through an endpoint,
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

    /// Sets a new filter for the Prometheus metrics exporter to collect metrics
    /// from remote endpoints. An empty vector means *none*.
    /// @note Has no effect when not configuring Prometheus export.
    void set_import_topics(std::vector<std::string> new_topics);

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

  /// @private
  endpoint(configuration config, endpoint_id this_peer);

  endpoint(endpoint&&) = delete;
  endpoint(const endpoint&) = delete;
  endpoint& operator=(endpoint&&) = delete;
  endpoint& operator=(const endpoint&) = delete;

  /// Calls `shutdown`.
  ~endpoint();

  /// Shuts down all background workers and blocks until all local subscribers
  /// and publishers have terminated. *Must* be the very last function call on
  /// this object before destroying it.
  /// @warning *Destroys* the underlying actor system. Calling *any* member
  ///          function afterwards except `shutdown` and the destructor is
  ///          undefined behavior.
  void shutdown();

  /// @returns a unique node id for this endpoint.
  endpoint_id node_id() const noexcept {
    return id_;
  }

  // --- peer management -------------------------------------------------------

  /// Listens at a specific port to accept remote peers.
  /// @param address The interface to listen at. If empty, listen on all
  ///                local interfaces.
  /// @param port The port to listen locally. If 0, the endpoint selects the
  ///             next available free port from the OS.
  /// @param err If non-null, stores the error when the functions returns 0.
  /// @param reuse_addr Causes Broker to set `SO_REUSEPORT` on UNIX systems if
  ///                   `true` (default). Has no effect on Windows.
  /// @returns The port the endpoint bound to or 0 on failure.
  uint16_t listen(const std::string& address = {}, uint16_t port = 0,
                  error* err = nullptr, bool reuse_addr = true);

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
  /// @returns True if connection was successfully set up.
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

  /// Initiates a peering with a remote endpoint, without waiting
  /// for the operation to complete.
  /// @param address The IP address of the remote endpoint.
  /// @param port The TCP port of the remote endpoint.
  /// @param retry If non-zero, seconds after which to retry if connection
  ///        cannot be established, or breaks.
  /// @return A `future` for catching the result at a later time.
  std::future<bool> peer_async(std::string address, uint16_t port,
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
  /// @returns A list containing a @ref peer_info entry for each known peer.
  std::vector<peer_info> peers() const;

  /// Retrieves a list of topics that peers have subscribed to on this endpoint.
  std::vector<topic> peer_subscriptions() const;

  // --- alternative client protocols ------------------------------------------

  /// Listens at @p port for incoming WebSocket connections.
  /// @param address The interface to listen at. If empty, listen on all
  ///                local interfaces.
  /// @param port The port to listen locally. If 0, the endpoint selects the
  ///             next available free port from the OS.
  ///
  /// @param err If non-null, stores the error when the functions returns 0.
  /// @param reuse_addr Causes Broker to set `SO_REUSEPORT` on UNIX systems if
  ///                   `true` (default). Has no effect on Windows.
  /// @returns The port the endpoint bound to or 0 on failure.
  uint16_t web_socket_listen(const std::string& address = {}, uint16_t port = 0,
                             error* err = nullptr, bool reuse_addr = true);

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
    using driver_t = detail::source_driver_impl_t<Init, Pull, AtEnd>;
    auto driver = std::make_shared<driver_t>(std::move(init), std::move(f),
                                             std::move(pred));
    return do_publish_all(std::move(driver));
  }

  /// Identical to ::publish_all, but does not guarantee that `init` is called
  /// before the function returns.
  template <class Init, class Pull, class AtEnd>
  [[deprecated("use publish_all() instead")]] worker
  publish_all_nosync(Init init, Pull f, AtEnd pred) {
    return publish_all(std::move(init), std::move(f), std::move(pred));
  }

  // --- subscribing events ----------------------------------------------------

  /// Returns a subscriber connected to this endpoint for receiving error and
  /// (optionally) status events.
  status_subscriber
  make_status_subscriber(bool receive_statuses = false,
                         size_t queue_size = defaults::subscriber::queue_size);

  // --- forwarding events -----------------------------------------------------

  // Forward remote events for given topics even if no local subscriber.
  void forward(std::vector<topic> ts);

  // --- subscribing data ------------------------------------------------------

  /// Returns a subscriber connected to this endpoint for the topics `ts`.
  subscriber
  make_subscriber(filter_type filter,
                  size_t queue_size = defaults::subscriber::queue_size);

  /// Starts a background worker from the given set of function that consumes
  /// incoming messages. The worker will run in the background, but `init` is
  /// guaranteed to be called before the function returns.
  template <class Init, class OnNext, class Cleanup>
  worker subscribe(filter_type filter, Init&& init, OnNext&& on_next,
                   Cleanup&& cleanup) {
    auto driver = detail::make_sink_driver(std::forward<Init>(init),
                                           std::forward<OnNext>(on_next),
                                           std::forward<Cleanup>(cleanup));
    return do_subscribe(std::move(filter), std::move(driver));
  }

  /// Starts a background worker from the given set of function that consumes
  /// incoming messages. The worker will run in the background.
  template <class OnNext>
  worker subscribe(filter_type filter, OnNext on_next) {
    return do_subscribe(std::move(filter),
                        detail::make_sink_driver([] {}, std::move(on_next),
                                                 [](const error&) {}));
  }

  template <class Init, class OnNext, class Cleanup>
  [[deprecated("use subscribe() instead")]] worker
  subscribe_nosync(filter_type filter, Init init, OnNext on_next,
                   Cleanup cleanup) {
    return subscribe(std::move(filter), std::move(init), std::move(on_next),
                     std::move(cleanup));
  }

  // --- data stores -----------------------------------------------------------

  /// Attaches and/or creates a *master* data store with a globally unique name.
  /// @param name The name of the master.
  /// @param type The type of backend to use.
  /// @param opts The options controlling backend construction.
  /// @returns A handle to the frontend representing the master or an error if
  ///          a master with *name* exists already.
  expected<store> attach_master(std::string name, backend type,
                                backend_options opts = backend_options());

  /// Attaches and/or creates a *clone* data store. Once attached, the clone
  /// tries to locate the master in the network. Note that this function does
  /// *not* wait until the master has been located. Hence, the clone may still
  /// raise a `no_such_master` error after some time.
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
  expected<store> attach_clone(std::string name, double resync_interval = 10.0,
                               double stale_interval = 300.0,
                               double mutation_buffer_interval = 120.0);

  // --- messaging -------------------------------------------------------------

  /// @private
  void send_later(worker who, timespan after, void* msg) {
    clock_->send_later(std::move(who), after, msg);
  }

  // --- setup and testing -----------------------------------------------------

  /// Initializes the OS socket layer if necessary. On Windows, this function
  /// calls `WSAStartup`. Does nothing on POSIX systems.
  static void init_socket_api();

  /// Releases resources for the OS socket layer if necessary. On Windows, this
  /// function calls `WSACleanup`. Does nothing on POSIX systems.
  static void deinit_socket_api();

  /// Initializes OpenSSL if necessary.
  static void init_ssl_api(); // note: implemented in internal/connector.cc

  /// Releases resources from OpenSSL.
  static void deinit_ssl_api(); // note: implemented in internal/connector.cc

  /// Initializes the host system by preparing all sub-systems and any global
  /// state required by broker. Calls @ref init_socket_api, @ref init_ssl_api
  /// and @ref configuration::init_global_state.
  static void init_system();

  /// Releases global state and resources.
  static void deinit_system();

  /// Automates system initialization when put at the top of `main` by calling
  /// @ref init_system in its constructor and @ref deinit_system in its
  /// destructor.
  struct system_guard {
    system_guard() {
      endpoint::init_system();
    }

    ~system_guard() {
      endpoint::deinit_system();
    }
  };

  // --await-peer-start
  /// Blocks execution of the current thread until either `whom` was added to
  /// the routing table and its subscription flooding reached this endpoint or a
  /// timeout occurs.
  /// @param whom ID of another endpoint.
  /// @param timeout An optional timeout for the configuring the maximum time
  ///                this function may block.
  /// @returns `true` if `whom` was added before the timeout, `false` otherwise.
  [[nodiscard]] bool
  await_peer(endpoint_id whom, timespan timeout = defaults::await_peer_timeout);

  /// Asynchronously runs `callback()` when `whom` was added to the routing
  /// table and its subscription flooding reached this endpoint.
  /// @param whom ID of another endpoint.
  /// @param callback A function object wrapping code for asynchronous
  ///                 execution. The argument for the callback is `true` if
  ///                 `whom` was added before the timeout, `false` otherwise.
  void await_peer(endpoint_id whom, std::function<void(bool)> callback,
                  timespan timeout = defaults::await_peer_timeout);
  // --await-peer-end

  /// Blocks execution of the current thread until the filter contains `what`.
  /// @param what Expected filter entry.
  /// @param timeout An optional timeout for the configuring the maximum time
  ///                this function may block.
  /// @returns `true` if `what` was added before the timeout, `false` otherwise.
  [[nodiscard]] bool
  await_filter_entry(const topic& what,
                     timespan timeout = defaults::await_peer_timeout);

  // -- worker management ------------------------------------------------------

  /// Blocks the current thread until `who` terminates.
  void wait_for(worker who);

  /// Stops a background worker.
  void stop(worker who);

  // --- properties ------------------------------------------------------------

  /// Queries whether the endpoint waits for masters and slaves on shutdown.
  bool await_stores_on_shutdown() const {
    constexpr auto flag = shutdown_options::await_stores_on_shutdown;
    return shutdown_options_.contains(flag);
  }

  /// Sets whether the endpoint waits for masters and slaves on shutdown.
  void await_stores_on_shutdown(bool x) {
    constexpr auto flag = shutdown_options::await_stores_on_shutdown;
    if (x)
      shutdown_options_.set(flag);
    else
      shutdown_options_.unset(flag);
  }

  /// Returns a configuration object for the metrics exporter.
  metrics_exporter_t metrics_exporter() {
    return metrics_exporter_t{this};
  }

  bool is_shutdown() const {
    return ctx_ == nullptr;
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

  /// Retrieves the current filter.
  filter_type filter() const;

protected:
  worker subscriber_;

private:
  worker do_subscribe(filter_type&& topics,
                      const detail::sink_driver_ptr& driver);

  worker do_publish_all(const detail::source_driver_ptr& driver);

  template <class F>
  worker make_worker(F fn);

  std::shared_ptr<internal::endpoint_context> ctx_;
  endpoint_id id_;
  worker core_;
  shutdown_options shutdown_options_;
  worker telemetry_exporter_;
  bool await_stores_on_shutdown_ = false;
  std::vector<worker> workers_;
  std::unique_ptr<clock> clock_;
  std::vector<std::unique_ptr<background_task>> background_tasks_;
};

} // namespace broker
