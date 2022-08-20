#pragma once

#include "broker/endpoint.hh"
#include "broker/internal/connector.hh"
#include "broker/internal/connector_adapter.hh"
#include "broker/internal/fwd.hh"
#include "broker/lamport_timestamp.hh"

#include <caf/disposable.hpp>
#include <caf/flow/observable.hpp>
#include <caf/make_counted.hpp>

#include <optional>
#include <unordered_map>

namespace broker::internal {

/// The core registers these message handlers:
class core_actor_state {
public:
  // -- member types -----------------------------------------------------------

  /// Bundles state for a single connected peer.
  struct peer_state {
    /// Handle for aborting inputs from this peer.
    caf::disposable in;

    /// Handle for aborting outputs to this peer.
    caf::disposable out;

    /// Network address as reported from the transport (usually TCP).
    network_info addr;

    /// Stores whether the connection to this peer has been closed. We set this
    /// flag instead of removing the peer entirely for implementing reconnects.
    bool invalidated = false;

    /// A logical timestamp for avoiding race conditions on this state.
    lamport_timestamp ts;

    peer_state() = delete;

    peer_state(caf::disposable in, caf::disposable out, network_info addr)
      : in(std::move(in)), out(std::move(out)), addr(std::move(addr)) {
      // nop
    }

    peer_state(peer_state&&) = default;

    peer_state& operator=(peer_state&&) = default;
  };

  /// Convenience alias for a map of @ref peer_state objects.
  using peer_state_map = std::unordered_map<endpoint_id, peer_state>;

  // -- constants --------------------------------------------------------------

  static inline const char* name = "broker.core";

  // --- constructors and destructors ------------------------------------------

  core_actor_state(caf::event_based_actor* self, endpoint_id this_peer,
                   filter_type initial_filter, endpoint::clock* clock = nullptr,
                   const domain_options* adaptation = nullptr,
                   connector_ptr conn = nullptr);

  ~core_actor_state();

  // -- initialization and tear down -------------------------------------------

  /// Creates the initial set of message handlers for `self`.
  caf::behavior make_behavior();

  /// Cleans up all state for an orderly shutdown.
  void shutdown(shutdown_options options);

  // -- convenience functions --------------------------------------------------

  /// Emits a status or error code.
  /// @private
  template <class EnumConstant>
  void emit(endpoint_info ep, EnumConstant code, const char* msg);

  /// Serializes a content of a data or command message and wraps the serialized
  /// data into a @ref packed_message.
  /// @private
  template <class T>
  packed_message pack(const T& msg);

  /// Deserializes a data or command message from the payload of `msg`.
  /// @private
  template <class T>
  std::optional<T> unpack(const packed_message& msg);

  /// Returns whether `x` has at least one remote subscriber.
  bool has_remote_subscriber(const topic& x) const noexcept;

  /// Checks whether peer with given `id` is subscribed to the topic `what`.
  bool is_subscribed_to(endpoint_id id, const topic& what);

  /// Returns the @ref network_info associated to given `id` if available.
  std::optional<network_info> addr_of(endpoint_id id) const;

  /// Returns the IDs of all connected peers.
  std::vector<endpoint_id> peer_ids() const;

  // -- callbacks --------------------------------------------------------------

  /// Called whenever this peer discovers a new peer in the network.
  /// @param peer_id ID of the new peer.
  /// @note The new peer gets stored in the routing table *before* calling this
  ///       member function.
  void peer_discovered(endpoint_id peer_id);

  /// Called whenever this peer established a new connection.
  /// @param peer_id ID of the newly connected peer.
  /// @param addr Network address for the peer.
  /// @note The new peer gets stored in the routing table *before* calling this
  ///       member function.
  void peer_connected(endpoint_id peer_id, const network_info& net);

  /// Called whenever this peer lost a connection to a remote peer.
  /// @param peer_id ID of the disconnected peer.
  /// @param addr Network address for the peer.
  void peer_disconnected(endpoint_id peer_id, const network_info& addr);

  /// Called whenever this peer removed a direct connection to a remote peer.
  /// @param peer_id ID of the removed peer.
  /// @param addr Network address for the peer.
  void peer_removed(endpoint_id peer_id, const network_info& addr);

  /// Called after removing the last path to `peer_id` from the routing table.
  /// @param peer_id ID of the (now unreachable) peer.
  void peer_unreachable(endpoint_id peer_id);

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
                      const std::string& type);

  // -- connection management --------------------------------------------------

  /// Tries to asynchronously connect to addr via the connector.
  void try_connect(const network_info& addr, caf::response_promise rp);

  /// Cleans up state when a peer is shutting down the connection.
  /// @param peer_id ID of the affected peer.
  /// @param ts Timestamp of the connection. This function does nothing if the
  ///           connection has been re-established in the meantime.
  /// @param reason Error code describing why the peer is shutting the
  ///               connection or a default-constructed error on regular
  ///               shutdown.
  void handle_peer_close_event(endpoint_id peer_id, lamport_timestamp ts,
                               caf::error& reason);

  // -- flow management --------------------------------------------------------

  /// Returns the `data_outputs` member, initializing it lazily if needed. We
  /// spin up this member only on demand to make sure we're not subscribing to
  /// the central merge point if we don't need to.
  caf::flow::observable<data_message> get_or_init_data_outputs();

  /// Returns the `command_outputs` member, initializing it lazily if needed. We
  /// spin up this member only on demand to make sure we're not subscribing to
  /// the central merge point if we don't need to.
  caf::flow::observable<command_message> get_or_init_command_outputs();

  /// Connects the input and output buffers for a new peer to our central merge
  /// point.
  caf::error init_new_peer(endpoint_id peer, const network_info& addr,
                           const filter_type& filter, node_consumer_res in_res,
                           node_producer_res out_res);

  /// Spin up a new background worker managing the socket and then dispatch to
  /// `init_new_peer` with the buffers that connect to the worker.
  caf::error init_new_peer(endpoint_id peer, const network_info& addr,
                           const filter_type& filter,
                           pending_connection_ptr conn);

  /// Connects the input and output buffers for a new client to our central
  /// merge point.
  caf::error init_new_client(const network_info& addr, const std::string& type,
                             filter_type filter, data_consumer_res in_res,
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
  void dispatch(endpoint_id receiver, packed_message msg);

  /// Broadcasts the local subscriptions to all peers.
  void broadcast_subscriptions();

  // -- unpeering --------------------------------------------------------------

  /// Disconnects a peer by demand of the user.
  void unpeer(endpoint_id peer_id);

  /// Disconnects a peer by demand of the user.
  void unpeer(const network_info& peer_addr);

  /// Disconnects a peer by demand of the user.
  void unpeer(peer_state_map::iterator i);

  // -- properties -------------------------------------------------------------

  /// Points to the actor itself.
  caf::event_based_actor* self;

  /// Identifies this peer in the network.
  endpoint_id id;

  /// Stores prefixes that have subscribers on this endpoint. This is shared
  /// with the connector, which needs access to the filter during handshake.
  shared_filter_ptr filter;

  /// Stores known filters from other peers.
  std::unordered_map<endpoint_id, filter_type> peer_filters;

  /// Stores whether this peer disabled forwarding, i.e., only appears as leaf
  /// node to other peers.
  bool disable_forwarding = false;

  /// Turns off status and error notifications for peering events.
  bool disable_notifications = false;

  /// Stores IDs of peers that we have no path to yet but some local actor is
  /// arleady waiting for. Usually for testing purposes.
  std::multimap<endpoint_id, caf::response_promise> awaited_peers;

  /// Enables manual time management by the user.
  endpoint::clock* clock;

  /// Stores all master actors created by this endpoint.
  std::unordered_map<std::string, caf::actor> masters;

  /// Stores all clone actors created by this endpoint.
  std::unordered_map<std::string, caf::actor> clones;

  /// Collects inputs from @ref broker::publisher objects.
  caf::flow::merger_impl_ptr<data_message> data_inputs;

  /// Collects inputs from data store objects.
  caf::flow::merger_impl_ptr<command_message> command_inputs;

  /// Provides central access to packed messages.
  caf::flow::merger_impl_ptr<node_message> central_merge;

  /// Pushes data to local @ref broker::subscriber objects.
  caf::flow::observable<data_message> data_outputs;

  /// Pushes commands to local data store objects.
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

  /// Stores the subscriptions for our input sources to allow us to cancel them.
  std::vector<caf::disposable> subscriptions;

  /// Bundles state for a subscriber that does not integrate into the flows.
  struct legacy_subscriber {
    std::shared_ptr<filter_type> filter;
    caf::disposable sub;
  };

  /// Associates handles to legacy subscribers with their state.
  std::map<caf::actor_addr, legacy_subscriber> legacy_subs;

  /// Time-to-live when sending messages.
  uint16_t ttl;

  /// Returns whether `shutdown` was called.
  bool shutting_down();
};

using core_actor = caf::stateful_actor<core_actor_state>;

} // namespace broker::internal
