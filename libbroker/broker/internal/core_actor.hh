#pragma once

#include "broker/endpoint.hh"
#include "broker/fwd.hh"
#include "broker/internal/connector.hh"
#include "broker/internal/connector_adapter.hh"
#include "broker/internal/fwd.hh"
#include "broker/internal/peering.hh"
#include "broker/internal/subscription_multimap.hh"
#include "broker/message.hh"

#include <caf/async/spsc_buffer.hpp>
#include <caf/callback.hpp>
#include <caf/disposable.hpp>
#include <caf/flow/item_publisher.hpp>
#include <caf/flow/observable.hpp>
#include <caf/make_counted.hpp>
#include <caf/uuid.hpp>

#include <array>
#include <optional>
#include <unordered_map>

namespace broker::internal {

/// The state of the core actor.
class core_actor_state {
public:
  // -- member types -----------------------------------------------------------

  using backpressure_overflow_strategy =
    caf::flow::backpressure_overflow_strategy;

  /// Convenience alias for a map of @ref peer_state objects.
  using peer_state_map = std::unordered_map<endpoint_id, peering_ptr>;

  /// Bundles message-related metrics that have a label dimension for the type.
  struct message_metrics_t {
    /// Counts how many messages were processed since starting the core.
    prometheus::Counter* processed = nullptr;

    void assign(prometheus::Counter* processed_instance) noexcept {
      processed = processed_instance;
    }
  };

  /// Bundles metrics for the core.
  struct metrics_t {
    explicit metrics_t(prometheus::Registry& reg);

    /// Keeps track of how many native peers are currently connected.
    prometheus::Gauge* native_connections = nullptr;

    /// Keeps track of how many WebSocket clients are currently connected.
    prometheus::Gauge* web_socket_connections = nullptr;

    /// Stores the metrics for all message types.
    std::array<message_metrics_t, 6> message_metric_sets;

    message_metrics_t& metrics_for(packed_message_type msg_type) {
      return message_metric_sets[static_cast<size_t>(msg_type)];
    }
  };

  /// The result of invoking a callback on a handler.
  enum class [[nodiscard]] handler_result {
    /// The callback was invoked successfully.
    ok,
    /// The callback was invoked but resulted in a terminal state.
    disposed,
  };

  /// Bundles state for sending and receiving messages to/from a peer.
  class handler {
  public:
    virtual ~handler();

    /// The ID of this handler. Can be a peer ID or a randomly generated UUID.
    endpoint_id id;

    /// The type of the handler, e.g. "native" or "websocket".
    std::string type = "native";

    /// Called whenever dispatching a message that matches the handler's filter.
    virtual handler_result offer(const node_message& msg) = 0;

    /// Callback for a downstream component demanding more messages.
    virtual handler_result add_demand(size_t demand) = 0;

    /// Tries to pull more messages from the handler.
    virtual handler_result pull(std::vector<node_message>& buf) = 0;

    /// Disposes of the handler.
    /// @note This function is always called implicitly if a callback returns
    ///       `handler_result::disposed`.
    virtual void dispose() = 0;
  };

  struct pull_observer {
    explicit pull_observer(std::vector<node_message>& storage) : buf(&storage) {
      // nop
    }

    template <class T>
    void on_next(const T& item) {
      buf->emplace_back(item);
    }

    void on_complete() {
      completed = true;
    }

    void on_error(const caf::error&) {
      failed = true;
    }

    std::vector<node_message>* buf;

    bool completed = false;

    bool failed = false;
  };

  template <class T>
  class handler_impl : public handler {
  public:
    using super = handler;

    using buffer_producer_ptr = caf::async::spsc_buffer_producer_ptr<T>;

    using buffer_consumer_ptr = caf::async::spsc_buffer_consumer_ptr<T>;

    handler_impl(size_t max_buffer_size,
                 backpressure_overflow_strategy overflow_policy)
      : max_buffer_size(max_buffer_size), overflow_policy(overflow_policy) {
      // nop
    }

    /// The consumer for reading messages from the peer's connection.
    buffer_consumer_ptr in;

    /// The producer for writing messages to the peer's connection.
    buffer_producer_ptr out;

    /// A buffer for messages that are not yet sent to the peer.
    std::deque<T> queue;

    /// The maximum number of messages that can be buffered.
    size_t max_buffer_size;

    backpressure_overflow_strategy overflow_policy;

    /// The number of messages that can be sent to the peer immediately.
    size_t demand = 0;

    /// A callback to be called when the peer is removed.
    caf::unique_callback_ptr<void()> on_remove;

    handler_result offer(const node_message& msg) override {
      if constexpr (std::is_same_v<T, node_message>) {
        return do_offer(msg);
      } else if constexpr (std::is_same_v<T, data_message>) {
        if (msg->type() == envelope_type::data) {
          return do_offer(msg->as_data());
        }
      } else {
        static_assert(std::is_same_v<T, command_message>);
        if (msg->type() == envelope_type::command) {
          return do_offer(msg->as_command());
        }
      }
      // If we reach this point, the message is not of the expected type and we
      // simply ignore it.
      return handler_result::ok;
    }

    handler_result add_demand(size_t new_demand) override {
      if (new_demand == 0) {
        return handler_result::ok;
      }
      demand += new_demand;
      while (demand > 0 && !queue.empty()) {
        auto n = std::min(demand, queue.size());
        auto i = queue.begin();
        auto e = i + static_cast<std::ptrdiff_t>(n);
        std::vector<T> items{i, e};
        queue.erase(i, e);
        out->push(std::move(items));
        demand -= n;
      }
      if (queue.empty() && !in) {
        dispose();
        return handler_result::disposed;
      }
      return handler_result::ok;
    }

    void dispose() override {
      if (in) {
        in->dispose();
      }
      if (out) {
        out->dispose();
      }
      if (on_remove) {
        (*on_remove)();
        on_remove = nullptr;
      }
    }

    handler_result pull(std::vector<node_message>& buf) override {
      if (!in) {
        return handler_result::ok;
      }
      pull_observer observer{buf};
      auto [again, pulled] = in->pull(100, observer);
      while (again && pulled == 100) {
        std::tie(again, pulled) = in->pull(100, observer);
      }
      if (!again) {
        in = nullptr;
        if (queue.empty()) {
          dispose();
          return handler_result::disposed;
        }
      }
      return handler_result::ok;
    }

    handler_result do_offer(T msg) {
      if (get_sender(msg) == id) {
        return handler_result::ok;
      }
      if (demand > 0) {
        out->push(std::move(msg));
        --demand;
        return handler_result::ok;
      }
      if (queue.size() < max_buffer_size) {
        queue.push_back(std::move(msg));
        return handler_result::ok;
      }
      switch (overflow_policy) {
        default: // backpressure_overflow_strategy::fail
          dispose();
          return handler_result::disposed;
        case backpressure_overflow_strategy::drop_newest:
          queue.pop_back();
          queue.push_back(std::move(msg));
          return handler_result::ok;
        case backpressure_overflow_strategy::drop_oldest:
          queue.pop_front();
          queue.push_back(std::move(msg));
          return handler_result::ok;
      }
    }
  };

  using handler_ptr = std::shared_ptr<handler>;

  // -- constants --------------------------------------------------------------

  static inline const char* name = "broker.core";

  // --- constructors and destructors ------------------------------------------

  core_actor_state(caf::event_based_actor* self,
                   prometheus_registry_ptr registry, endpoint_id this_peer,
                   filter_type initial_filter, endpoint::clock* clock = nullptr,
                   const domain_options* adaptation = nullptr,
                   connector_ptr conn = nullptr);

  ~core_actor_state();

  // -- initialization and tear down -------------------------------------------

  /// Creates the initial set of message handlers for `self`.
  caf::behavior make_behavior();

  /// Initiates an orderly shutdown.
  void shutdown(shutdown_options options);

  /// Cleans up all state.
  void finalize_shutdown();

  // -- convenience functions --------------------------------------------------

  /// Emits a status or error code.
  /// @private
  template <class Info, class EnumConstant>
  void emit(Info&& ep, EnumConstant code, const char* msg);

  /// Returns whether `x` has at least one remote subscriber.
  bool has_remote_subscriber(const topic& x) const noexcept;

  /// Checks whether peer with given `id` is subscribed to the topic `what`.
  bool is_subscribed_to(endpoint_id id, const topic& what);

  /// Returns the @ref network_info associated to given `id` if available.
  std::optional<network_info> addr_of(endpoint_id id) const;

  /// Returns the IDs of all connected peers.
  std::vector<endpoint_id> peer_ids() const;

  /// Creates a snapshot for the current values of the message metrics.
  table message_metrics_snapshot() const;

  /// Creates a snapshot for the peering statistics.
  table peer_stats_snapshot() const;

  /// Creates a snapshot that summarizes the current status of the core.
  table status_snapshot() const;

  // -- callbacks --------------------------------------------------------------

  /// Called whenever the user tried to unpeer from an unknown peer.
  /// @param xs Either a peer ID, an actor handle or a network info.
  void cannot_remove_peer(endpoint_id x);

  /// Called whenever the user tried to unpeer from an unknown peer.
  /// @param xs Either a peer ID, an actor handle or a network info.
  void cannot_remove_peer(const network_info& x);

  /// Called whenever establishing a connection to a remote peer failed.
  /// @param xs Either a peer ID or a network info.
  void peer_unavailable(const network_info& x);

  /// Called whenever a new client connected.
  void client_added(endpoint_id client_id, const network_info& addr,
                    const std::string& type);

  /// Called whenever a client disconnected.
  void client_removed(endpoint_id client_id, const network_info& addr,
                      const std::string& type, const caf::error& reason,
                      bool removed);

  /// Called whenever a peer has demand for more messages.
  void on_demand(const handler_ptr& peer, size_t demand);

  /// Called whenever a peer cancels its subscription.
  void on_cancel(const handler_ptr& peer);

  /// Called whenever a peer signals that it enqueued new messages.
  void on_data(const handler_ptr& peer);

  /// Called to remove a state from all filters and the peer map.
  void erase(const handler_ptr& peer);

  // -- connection management --------------------------------------------------

  /// Tries to asynchronously connect to addr via the connector.
  void try_connect(const network_info& addr, caf::response_promise rp);

  // -- flow management --------------------------------------------------------

  /// Connects the input and output buffers for a new peer to our central merge
  /// point.
  caf::error init_new_peer(endpoint_id peer, const network_info& addr,
                           const filter_type& filter, node_consumer_res in_res,
                           node_producer_res out_res);

  caf::error init_new_peer(endpoint_id peer, const network_info& addr,
                           const filter_type& filter, chunk_consumer_res in_res,
                           chunk_producer_res out_res);

  /// Spin up a new background worker managing the socket and then dispatch to
  /// `init_new_peer` with the buffers that connect to the worker.
  caf::error init_new_peer(endpoint_id peer, const network_info& addr,
                           const filter_type& filter,
                           const pending_connection_ptr& conn);

  /// Connects the input and output buffers for a new client to our central
  /// merge point.
  caf::error init_new_client(const network_info& addr, std::string& type,
                             const filter_type& filter,
                             data_consumer_res in_res,
                             data_producer_res out_res);

  // -- topic management -------------------------------------------------------

  /// Adds `what` to the local filter and also forwards the subscription to
  /// connected peers.
  void subscribe(const filter_type& what);

  // -- data store management --------------------------------------------------

  /// Returns whether a master for `name` probably exists already on one of our
  /// peers.
  bool has_remote_master(const std::string& name) const;

  /// Attaches a master for given store to this peer.
  caf::result<caf::actor> attach_master(const std::string& name,
                                        backend backend_type,
                                        backend_options opts);

  /// Attaches a clone for given store to this peer.
  caf::result<caf::actor> attach_clone(const std::string& name,
                                       double resync_interval,
                                       double stale_interval,
                                       double mutation_buffer_interval);

  /// Terminates all masters and clones by sending exit messages to the
  /// corresponding actors.
  void shutdown_stores();

  // -- dispatching of messages ------------------------------------------------

  /// Dispatches `msg` to `receiver` regardless of its subscriptions.
  /// @returns `true` on success, `false` if no peering to `receiver` exists.
  void dispatch(const node_message& msg);

  /// Broadcasts the local subscriptions to all peers.
  void broadcast_subscriptions();

  // -- unpeering --------------------------------------------------------------

  /// Disconnects a peer by demand of the user.
  void unpeer(endpoint_id peer_id);

  /// Disconnects a peer by demand of the user.
  void unpeer(const network_info& peer_addr);

  // -- properties -------------------------------------------------------------

  size_t peer_buffer_size();

  backpressure_overflow_strategy peer_overflow_policy();

  size_t web_socket_buffer_size();

  backpressure_overflow_strategy web_socket_overflow_policy();

  // -- member variables -------------------------------------------------------

  /// Points to the actor itself.
  caf::event_based_actor* self;

  /// Identifies this peer in the network.
  endpoint_id id;

  /// Stores the subscriptions from our handlers.
  subscription_multimap<handler_ptr> handler_subscriptions;

  /// Stores the state for all handlers.
  std::unordered_map<endpoint_id, handler_ptr> handlers;

  /// Reusable buffer for pulling messages from handlers.
  std::vector<node_message> pull_buffer;

  /// Reusable buffer for selecting handlers.
  std::vector<handler_ptr> handler_selection;

  // -- OLD

  /// Checks whether a message has a local sender.
  bool is_local(const node_message& msg) const noexcept {
    auto sender = get_sender(msg);
    return !sender || sender == id;
  }

  /// Stores prefixes that have subscribers on this endpoint. This is shared
  /// with the connector, which needs access to the filter during handshake.
  shared_filter_ptr filter;

  /// Stores whether this peer disabled forwarding, i.e., only appears as leaf
  /// node to other peers.
  bool disable_forwarding = false;

  /// Stores IDs of peers that we have no path to yet but some local actor is
  /// arleady waiting for. Usually for testing purposes.
  std::multimap<endpoint_id, caf::response_promise> awaited_peers;

  /// Enables manual time management by the user.
  endpoint::clock* clock;

  /// Stores a reference to the metrics registry.
  prometheus_registry_ptr registry;

  /// Caches pointers to the Broker metrics.
  metrics_t metrics;

  /// Stores all master actors created by this endpoint.
  std::unordered_map<std::string, caf::actor> masters;

  /// Stores all clone actors created by this endpoint.
  std::unordered_map<std::string, caf::actor> clones;

  /// An input from a hub. The first element is the hub ID, the second element
  /// is the message
  using hub_input = std::pair<hub_id, data_envelope_ptr>;

  /// Pushes flows into the hub merge point.
  caf::flow::item_publisher<caf::flow::observable<hub_input>> hub_inputs;

  /// The output of `hub_inputs`.
  caf::flow::observable<hub_input> hub_merge;

  /// Pushes messages into the flow. This is marked as unsafe, because we push
  /// inputs from the mailbox directly into the buffer without a back-pressure
  /// for the senders.
  caf::flow::item_publisher<node_message> unsafe_inputs;

  /// Pushes flows into the central merge point.
  caf::flow::item_publisher<caf::flow::observable<node_message>> flow_inputs;

  /// The output of `flow_inputs`.
  caf::flow::observable<node_message> central_merge;

  /// Pushes data messages into the flow.
  caf::flow::observable<data_message> data_outputs;

  /// Pushes command messages into the flow.
  caf::flow::observable<command_message> command_outputs;

  /// Handle to the background worker for establishing peering relations.
  std::unique_ptr<connector_adapter> adapter;

  /// Handles for aborting flows on unpeering.
  peer_state_map peers;

  /// Synchronizes information about the current status of a peering with the
  /// connector.
  detail::shared_peer_status_map_ptr peer_statuses =
    std::make_shared<detail::peer_status_map>();

  /// Buffer for serializing messages. Having this as a member allows us to
  /// re-use the same heap-allocated buffer instead of always allocating fresh
  /// memory regions over and over again.
  caf::byte_buffer buf;

  using disposable_list = std::vector<caf::disposable>;

  /// Stores the subscriptions for our input sources to allow us to cancel them.
  std::map<endpoint_id, disposable_list> subscriptions;

  /// Bundles state for a subscriber that does not integrate into the flows.
  struct legacy_subscriber {
    std::shared_ptr<filter_type> filter;
    caf::disposable sub;
  };

  /// Associates handles to legacy subscribers with their state.
  std::map<caf::actor_addr, legacy_subscriber> legacy_subs;

  /// Time-to-live when sending messages.
  uint16_t ttl;

  /// When shutting down, this scheduled action forces disconnects on all peers
  /// after the timeout.
  caf::disposable shutting_down_timeout;

  /// Returns whether `shutdown` was called.
  bool shutting_down();

  /// Returns the metrics set for a given message type.
  message_metrics_t& metrics_for(packed_message_type msg_type) {
    return metrics.metrics_for(msg_type);
  }

  /// Counts messages that were published directly via message, i.e., without
  /// using the back-pressure of flows.
  int64_t published_via_async_msg = 0;

  struct hub_state {
    ~hub_state() {
      in.dispose();
      out.dispose();
    }

    filter_type filter;
    caf::disposable in;
    caf::disposable out;
  };

  using hub_state_ptr = std::shared_ptr<hub_state>;

  void drop_hub_input(hub_id id);

  void drop_hub_output(hub_id id);

  std::unordered_map<hub_id, hub_state_ptr> hubs;

private:
  template <class T>
  caf::error do_init_new_peer(endpoint_id peer_id, const network_info& addr,
                              const filter_type& filter,
                              caf::async::consumer_resource<T> in_res,
                              caf::async::producer_resource<T> out_res);
};

using core_actor = caf::stateful_actor<core_actor_state>;

} // namespace broker::internal
