#pragma once

#include "broker/detail/prefix_matcher.hh"
#include "broker/endpoint.hh"
#include "broker/endpoint_id.hh"
#include "broker/fwd.hh"
#include "broker/internal/connection_status.hh"
#include "broker/internal/connector.hh"
#include "broker/internal/connector_adapter.hh"
#include "broker/internal/fwd.hh"
#include "broker/internal/hub_handler.hh"
#include "broker/internal/message_handler.hh"
#include "broker/internal/message_handler_offer_result.hh"
#include "broker/internal/message_handler_pull_result.hh"
#include "broker/internal/message_handler_type.hh"
#include "broker/internal/message_provider.hh"
#include "broker/internal/peering_handler.hh"
#include "broker/internal/pull_observer.hh"
#include "broker/internal/store_handler.hh"
#include "broker/internal/subscription_multimap.hh"
#include "broker/internal/web_socket_client_handler.hh"
#include "broker/internal/wire_format.hh"
#include "broker/logger.hh"
#include "broker/message.hh"
#include "broker/overflow_policy.hh"

#include <caf/async/spsc_buffer.hpp>
#include <caf/callback.hpp>
#include <caf/chunk.hpp>
#include <caf/disposable.hpp>
#include <caf/flow/observable.hpp>
#include <caf/make_counted.hpp>
#include <caf/uuid.hpp>

#include <prometheus/gauge.h>

#include <array>
#include <optional>
#include <unordered_map>
#include <unordered_set>

namespace broker::internal {

/// Trait for getting the value and ID type of a handler.
template <message_handler_type>
struct handler_type_trait;

template <>
struct handler_type_trait<message_handler_type::store> {
  using value_type = command_message;
  using id_type = std::string;
};

template <>
struct handler_type_trait<message_handler_type::client> {
  using value_type = data_message;
  using id_type = endpoint_id;
};

template <>
struct handler_type_trait<message_handler_type::peering> {
  using value_type = caf::chunk;
  using id_type = endpoint_id;
};

template <>
struct handler_type_trait<message_handler_type::hub> {
  using value_type = data_message;
  using id_type = hub_id;
};

/// Convenience alias for the value type of a handler.
template <message_handler_type T>
using handler_value_type = typename handler_type_trait<T>::value_type;

/// Convenience alias for the ID type of a handler.
template <message_handler_type T>
using handler_id_type = typename handler_type_trait<T>::id_type;

/// The state of the core actor.
class core_actor_state {
public:
  // -- member types -----------------------------------------------------------

  /// Bundles message-related metrics that have a label dimension for the type.
  struct message_metrics_t {
    /// Counts how many messages were processed since starting the core.
    prometheus::Counter* processed = nullptr;
  };

  /// Bundles metrics for the core.
  struct metrics_t {
    explicit metrics_t(prometheus::Registry& reg);

    /// Counts how many times we disconnected from a peer.
    prometheus::Counter* peer_disconnects;

    /// Keeps track of how many messages are currently buffered.
    prometheus::Gauge* buffered_messages;

    /// Keeps track of how many native peers are currently connected.
    prometheus::Gauge* native_connections;

    /// Keeps track of how many WebSocket clients are currently connected.
    prometheus::Gauge* web_socket_connections;

    /// Stores the metrics for all message types.
    std::array<message_metrics_t, 5> message_metric_sets;

    const message_metrics_t& metrics_for(packed_message_type msg_type) const {
      // Note: the enum starts at 1. Hence, we subtract 1 to get a 0-based index
      // into our array.
      return message_metric_sets[static_cast<size_t>(msg_type) - 1];
    }
  };

  /// Calls the given function with the handler cast to the appropriate type.
  template <class Fn>
  auto with_subtype(const message_handler_ptr& ptr, Fn&& fn) {
    switch (ptr->type()) {
      case message_handler_type::store:
        return fn(std::static_pointer_cast<store_handler>(ptr));
      case message_handler_type::client:
        return fn(std::static_pointer_cast<web_socket_client_handler>(ptr));
      case message_handler_type::peering:
        return fn(std::static_pointer_cast<peering_handler>(ptr));
      case message_handler_type::hub:
      case message_handler_type::subscriber:
      case message_handler_type::publisher:
        return fn(std::static_pointer_cast<hub_handler>(ptr));
      default:
        throw std::logic_error("invalid handler type");
    }
  }

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

  /// Checks whether the peer with the given `id` is subscribed to the topic
  /// `what`.
  bool is_subscribed_to(endpoint_id id, const topic& what);

  /// Returns the @ref network_info associated with the given `id` if available.
  std::optional<network_info> addr_of(endpoint_id id) const;

  /// Creates a snapshot for the current values of the message metrics.
  table message_metrics_snapshot() const;

  /// Creates a snapshot for the peering statistics.
  table peer_stats_snapshot() const;

  /// Creates a snapshot that summarizes the current status of the core.
  table status_snapshot() const;

  /// Sets up a handler by connecting its input and output buffers as well as
  /// adding its subscriptions to the handler subscriptions map.
  template <class T>
  void setup(const std::shared_ptr<T>& ptr,
             caf::async::consumer_resource<typename T::value_type> in_res,
             caf::async::producer_resource<typename T::value_type> out_res,
             const filter_type& filter) {
    // Call `on_data` whenever there is activity on the input buffer.
    if (in_res) {
      ptr->in = in_res.consume_on(self, [this, ptr](auto&) { on_data(ptr); });
    }
    // Call `on_demand` if the peer requests more messages and
    // `on_output_closed` when the peer closes the output buffer.
    if (out_res) {
      ptr->out = out_res.produce_on(
        self, [this, ptr](auto&, size_t demand) { on_demand(ptr, demand); },
        [this, ptr](auto&) {
          ptr->out = nullptr;
          on_output_closed(ptr);
        });
    }
    // Extend our filter with the new handler.
    for (auto& sub : filter) {
      handler_subscriptions.insert(sub.string(), ptr);
    }
  }

  /// Calls `offer` on the given handler with the message provider and handles
  /// the result, removing the handler if it disconnects or terminates.
  /// @returns `true` if the message was accepted by the handler, `false`
  /// otherwise.
  bool offer_msg(const message_handler_ptr& ptr);

  // -- callbacks --------------------------------------------------------------

  /// Called whenever the user tries to unpeer from an unknown peer.
  /// @param x Either a peer ID, an actor handle or a network info.
  void cannot_remove_peer(endpoint_id x);

  /// Called whenever the user tries to unpeer from an unknown peer.
  /// @param x Either a peer ID, an actor handle or a network info.
  void cannot_remove_peer(const network_info& x);

  /// Called whenever establishing a connection to a remote peer failed.
  /// @param x Either a peer ID or a network info.
  void peer_unavailable(const network_info& x);

  /// Called whenever a new client connects.
  void client_added(endpoint_id client_id, const network_info& addr,
                    const std::string& type);

  /// Called whenever a client disconnects.
  void client_removed(endpoint_id client_id, const network_info& addr,
                      const std::string& type, const caf::error& reason,
                      bool removed);

  /// Called whenever we remove a peer by demand of the user.
  void on_peer_disconnect(const peering_handler_ptr& ptr);

  /// Called whenever a peer has demand for more messages.
  void on_demand(const message_handler_ptr& peer, size_t demand);

  /// Called whenever a peer signals that it enqueues new messages.
  void on_data(const message_handler_ptr& ptr);

  /// Called whenever a peer signals that it enqueues new messages.
  void on_data(const peering_handler_ptr& peer);

  /// Called when triggering an overflow and the handler is configured to
  /// disconnect.
  void on_overflow_disconnect(const message_handler_ptr& ptr);

  /// Called whenever a handler closes its input buffer.
  void on_input_closed(const message_handler_ptr& ptr);

  /// Called whenever a handler closes its output buffer.
  void on_output_closed(const message_handler_ptr& ptr);

  /// Called from `on_overflow_disconnect`, `on_input_closed`, or
  /// `on_output_closed` when a handler was disposed.
  void on_handler_disposed(const message_handler_ptr& ptr);

  /// Called to remove a handler from its container.
  void erase(const message_handler_ptr& what);

  /// Removes the handler from `peerings`.
  void do_erase(const peering_handler_ptr& what);

  /// Removes the handler from `hubs`.
  void do_erase(const hub_handler_ptr& what);

  /// Removes the handler from `masters` or `clones`.
  void do_erase(const store_handler_ptr& what);

  /// Removes the handler from `clients`.
  void do_erase(const web_socket_client_handler_ptr& what);

  // -- connection management --------------------------------------------------

  /// Tries to asynchronously connect to addr via the connector.
  void try_connect(const network_info& addr, caf::response_promise rp);

  // -- flow management --------------------------------------------------------

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

  /// Returns whether a master for `name` probably already exists on one of our
  /// peers.
  bool has_remote_master(const std::string& name) const;

  /// Attaches a master for the given store to this peer.
  caf::result<caf::actor> attach_master(const std::string& name,
                                        backend backend_type,
                                        backend_options opts);

  /// Attaches a clone for the given store to this peer.
  caf::result<caf::actor> attach_clone(const std::string& name,
                                       double resync_interval,
                                       double stale_interval,
                                       double mutation_buffer_interval);

  /// Terminates all masters and clones by sending exit messages to the
  /// corresponding actors.
  void shutdown_stores();

  // -- dispatching of messages ------------------------------------------------

  /// Dispatches `msg` to all subscribers based on the message topic.
  void dispatch(const node_message& msg);

  /// Dispatches `msg` to all subscribers based on the message topic, except for
  /// `from`.
  void dispatch_from(const node_message& msg, const message_handler_ptr& from);

  /// Broadcasts the local subscriptions to all peers.
  void broadcast_subscriptions();

  // -- unpeering --------------------------------------------------------------

  /// Disconnects a peer by demand of the user.
  void unpeer(endpoint_id peer_id);

  /// Disconnects a peer by demand of the user.
  void unpeer(const network_info& peer_addr);

  // -- properties -------------------------------------------------------------

  size_t store_buffer_size();

  size_t peer_buffer_size();

  overflow_policy peer_overflow_policy();

  size_t web_socket_buffer_size();

  overflow_policy web_socket_overflow_policy();

  size_t hub_buffer_size();

  /// Returns the handlers for all peerings.
  auto peering_handlers() const {
    std::vector<peering_handler_ptr> result;
    result.reserve(peerings.size());
    for (auto& kvp : peerings) {
      result.push_back(kvp.second);
    }
    return result;
  }

  bool send_bye_message(const peering_handler_ptr& ptr);

  // -- member variables -------------------------------------------------------

  /// Points to the actor itself.
  caf::event_based_actor* self;

  /// Identifies this peer in the network.
  endpoint_id id;

  /// Stores the subscriptions from our non-peer handlers.
  subscription_multimap<message_handler_ptr> handler_subscriptions;

  /// Reusable message provider.
  message_provider msg_provider;

  /// Stores all master actors created by this endpoint.
  std::unordered_map<std::string, store_handler_ptr> masters;

  /// Stores all clone actors created by this endpoint.
  std::unordered_map<std::string, store_handler_ptr> clones;

  /// Stores the state for all peerings.
  std::unordered_map<endpoint_id, peering_handler_ptr> peerings;

  /// Stores the state for all clients.
  std::unordered_map<endpoint_id, web_socket_client_handler_ptr> clients;

  /// Stores the state for all hubs.
  std::unordered_map<hub_id, hub_handler_ptr> hubs;

  /// Reusable buffer for passing to `handler_subscriptions.select()`.
  std::vector<message_handler_ptr> selection_buffer;

  /// Stores whether this peer has disabled forwarding, i.e., only appears as a
  /// leaf node to other peers.
  bool disable_forwarding = false;

  /// Stores prefixes that have subscribers on this endpoint. This is shared
  /// with the connector, which needs access to the filter during handshake.
  shared_filter_ptr filter;

  /// Stores IDs of peers that we have no path to yet but some local actor is
  /// already waiting for. Usually for testing purposes.
  std::multimap<endpoint_id, caf::response_promise> awaited_peers;

  /// Enables manual time management by the user.
  endpoint::clock* clock;

  /// Stores a reference to the metrics registry.
  prometheus_registry_ptr registry;

  /// Caches pointers to the Broker metrics.
  metrics_t metrics;

  /// Handle to the background worker for establishing peering relations.
  std::unique_ptr<connector_adapter> adapter;

  /// Synchronizes information about the current status of a peering with the
  /// connector.
  detail::shared_peer_status_map_ptr peer_statuses =
    std::make_shared<detail::peer_status_map>();

  /// Time-to-live when sending messages.
  uint16_t ttl;

  /// When shutting down, this scheduled action forces disconnects on all peers
  /// after the timeout.
  caf::disposable shutting_down_timeout;

  /// Counts messages that were published directly via message publishing, i.e.,
  /// without using the back-pressure of flows.
  int64_t published_via_async_msg = 0;

  /// Stores whether `shutdown` was called.
  bool shutting_down_ = false;
};

using core_actor = caf::stateful_actor<core_actor_state>;

} // namespace broker::internal
