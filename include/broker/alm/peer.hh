#pragma once

#include <map>
#include <vector>

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/behavior.hpp>
#include <caf/event_based_actor.hpp>

#include "broker/alm/multipath.hh"
#include "broker/alm/routing_table.hh"
#include "broker/atoms.hh"
#include "broker/defaults.hh"
#include "broker/detail/assert.hh"
#include "broker/detail/hash.hh"
#include "broker/detail/lift.hh"
#include "broker/detail/prefix_matcher.hh"
#include "broker/error.hh"
#include "broker/filter_type.hh"
#include "broker/logger.hh"
#include "broker/message.hh"
#include "broker/network_info.hh"
#include "broker/shutdown_options.hh"

namespace broker::alm {

/// Base class that represents a Broker peer in the network. This class
/// implements subscription and path management for the overlay. Member
/// functions for data transport as well as shipping data to local subscribers
/// remain pure virtual.
///
/// The derived class *must* provide the following interface:
///
/// ~~~
/// class Derived {
///   template <class... Ts>
///   void send(const caf::actor& receiver, Ts&&... xs);
///
///   const PeerId& id() const noexcept;
/// };
/// ~~~
///
/// The derived class *can* extend any of the callback member functions by
/// hiding the implementation of ::peer.
///
/// The peer registers these message handlers:
///
/// ~~~
/// (atom::get, atom::id) -> endpoint_id
/// => id()
///
/// (atom::publish, data_message msg) -> void
/// => publish_data(msg)
///
/// (atom::publish, command_message msg) -> void
/// => publish_command(msg)
///
/// (atom::subscribe, filter_type filter) -> void
/// => subscribe(filter)
///
/// (atom::publish, node_message msg) -> void
/// => handle_publication(msg)
///
/// (atom::subscribe, endpoint_id_list path, filter_type filter, lamport_timestamp)
/// -> void
/// => handle_filter_update(path, filter, t)
///
/// (atom::revoke, revoker, ts, hop) -> void
/// => handle_path_revocation(revoker, ts, hop)
///
/// (atom::await, PeerId) -> void
/// => await_endpoint()
/// ~~~
class peer {
public:
  // -- constants --------------------------------------------------------------

  /// Checks whether move-assigning an ID to the peer results in a `noexcept`
  /// operation.
  static constexpr bool has_nothrow_assignable_id
    = std::is_nothrow_move_assignable<endpoint_id>::value;

  // -- nested types -----------------------------------------------------------

  struct revocations_type {
    alm::revocations<endpoint_id> entries;

    caf::timespan aging_interval;

    caf::timespan max_age;

    caf::actor_clock::time_point next_aging_cycle;
  };

  // -- constructors, destructors, and assignment operators --------------------

  explicit peer(caf::event_based_actor* selfptr);

  peer() = delete;

  peer(const peer&) = delete;

  peer& operator=(const peer&) = delete;

  virtual ~peer();

  // -- properties -------------------------------------------------------------

  caf::event_based_actor* self() noexcept {
    return self_;
  }

  const endpoint_id& id() const noexcept {
    return id_;
  }

  void id(endpoint_id new_id) noexcept(has_nothrow_assignable_id) {
    id_ = std::move(new_id);
  }

  auto& tbl() noexcept {
    return tbl_;
  }

  const auto& tbl() const noexcept {
    return tbl_;
  }

  const auto& filter() const noexcept {
    return filter_;
  }

  const auto& peer_filters() const noexcept {
    return peer_filters_;
  }

  auto peer_filter(const endpoint_id& x) const {
    auto i = peer_filters_.find(x);
    if (i != peer_filters_.end())
      return i->second.entry;
    return filter_type{};
  }

  auto timestamp() const noexcept {
    return timestamp_;
  }

  auto peer_ids() const {
    endpoint_id_list result;
    for (auto& kvp : tbl_)
      result.emplace_back(kvp.first);
    return result;
  }

  auto& revocations() const noexcept {
    return revocations_;
  }

  bool disable_forwarding() const noexcept {
    return disable_forwarding_;
  }

  void disable_forwarding(bool value) noexcept {
    disable_forwarding_ = value;
  }

  // -- convenience functions for subscription information ---------------------

  bool has_remote_subscriber(const topic& x) const noexcept;

  static bool contains(const endpoint_id_list& ids, const endpoint_id& id);

  // -- topic management -------------------------------------------------------

  virtual void subscribe(const filter_type& what);

  // -- additional dispatch overloads ------------------------------------------

  /// @private
  template <class T>
  bool dispatch_to_impl(T&& msg, endpoint_id receiver);

  /// Dispatches `msg` to `receiver`, ignoring subscription filters.
  /// @returns `true` on success, `false` if no path to the receiver exists.
  bool dispatch_to(data_message msg, endpoint_id receiver);

  /// Dispatches `msg` to `receiver`, ignoring subscription filters.
  /// @returns `true` on success, `false` if no path to the receiver exists.
  bool dispatch_to(command_message msg, endpoint_id receiver);

  // -- flooding ---------------------------------------------------------------

  /// Checks whether a path and its associated vector timestamp are non-empty,
  /// loop-free and not revoked.
  bool valid(endpoint_id_list& path, vector_timestamp path_ts);

  /// Removes all entries from the revocations that exceeded their maximum age.
  /// The purpose of this function is to clean up state periodically to avoid
  /// unbound growth of the revocations. Calling it at fixed intervals is not
  /// required. Triggering aging on peer messages suffices, since only peer
  /// messages can grow the revocations in the first place.
  void age_revocations();

  /// Request form @p src for receiving our current filter.
  /// @param src The sender of the message.
  void handle_filter_request(const endpoint_id& src);

  /// Updates the filter for @p src.
  /// @param src The sender of the message.
  /// @param ts The time stamp of the sender, i.e., @p src.
  /// @param filter The new filter.
  /// @note increases the local time if the routing table changes.
  void handle_filter_update(const endpoint_id& src, lamport_timestamp ts,
                            const filter_type& new_filter);

  /// Adds the reverse `path` to the routing table. This peer triggers a path
  /// discovery on its own when discovering new peers in the network.
  /// @param path The path this message traveled on to reach this peer.
  /// @param ts The time stamp of all peers at the time of sending / forwarding.
  /// @note increases the local time if the routing table changes.
  void handle_path_discovery(endpoint_id_list& path, vector_timestamp& ts);

  /// Removes any path from the routing table for the removed link.
  /// @param hop The peer that revokes a connection link.
  /// @param ts The time stamp of the sender, i.e., @p hop.
  /// @param revoked_hop The peer that lost its connection to @p hop.
  /// @note increases the local time if the routing table changes.
  void handle_path_revocation(endpoint_id_list& path, vector_timestamp& path_ts,
                              const endpoint_id& revoked_hop);

  // -- interface to the transport ---------------------------------------------

  /// Publishes a new filter to *all* known peers.
  /// @param ts The logical time for the filter-update-event.
  /// @param new_filter The new filter to apply to the origin of the update.
  virtual void
  publish_filter_update(lamport_timestamp ts, const filter_type& new_filter)
    = 0;

  /// Publishes the filter to the peer @p dst.
  /// @param dst Destinationfor the filter update.
  /// @param ts The logical time for the filter-update-event.
  /// @param new_filter The new filter to apply to the origin of the update.
  virtual void publish_filter_update(endpoint_id dst, lamport_timestamp ts,
                                     const filter_type& new_filter)
    = 0;

  /// Triggers a path discovery (flooding).
  virtual void trigger_path_discovery() = 0;

  /// Triggers a path discovery (flooding).
  virtual void forward_path_discovery(endpoint_id next_hop,
                                      const endpoint_id_list& path,
                                      const vector_timestamp& ts)
    = 0;

  /// Triggers a path revocation (flooding).
  /// @param lost_peer ID of the peer that we lost connection to.
  virtual void trigger_path_revocation(const endpoint_id& lost_peer) = 0;

  /// Triggers a path discovery (flooding).
  virtual void forward_path_revocation(endpoint_id next_hop,
                                       const endpoint_id_list& path,
                                       const vector_timestamp& ts,
                                       const endpoint_id& lost_peer)
    = 0;

  /// Triggers filter requests (source-routed messages).
  virtual void trigger_filter_requests() = 0;

  /// Publishes @p msg to all local subscribers.
  /// @param msg The published data.
  virtual void publish_locally(const data_message& msg) = 0;

  /// Publishes @p msg to all local subscribers.
  /// @param msg The published command.
  virtual void publish_locally(const command_message& msg) = 0;

  virtual void dispatch(const data_message& msg) = 0;

  virtual void dispatch(const command_message& msg) = 0;

  virtual void dispatch(alm::multipath path, const data_message& msg) = 0;

  virtual void dispatch(alm::multipath path, const command_message& msg) = 0;

  // -- callbacks --------------------------------------------------------------

  /// Called whenever this peer discovers a new peer in the network.
  /// @param peer_id ID of the new peer.
  /// @note The new peer gets stored in the routing table *before* calling this
  ///       member function.
  virtual void peer_discovered(const endpoint_id& peer_id);

  /// Called whenever this peer established a new connection.
  /// @param peer_id ID of the newly connected peer.
  /// @note The new peer gets stored in the routing table *before* calling this
  ///       member function.
  virtual void peer_connected(const endpoint_id& peer_id);

  /// Called whenever this peer lost a connection to a remote peer.
  /// @param peer_id ID of the disconnected peer.
  /// @param reason None if we closed the connection gracefully, otherwise
  ///               contains the transport-specific error code.
  virtual void peer_disconnected(const endpoint_id& peer_id,
                                 const error& reason);

  /// Called whenever this peer removed a direct connection to a remote peer.
  /// @param peer_id ID of the removed peer.
  virtual void peer_removed(const endpoint_id& peer_id);

  /// Called after removing the last path to `peer_id` from the routing table.
  /// @param peer_id ID of the (now unreachable) peer.
  virtual void peer_unreachable(const endpoint_id& peer_id);

  /// Called whenever the user tried to unpeer from an unknown peer.
  /// @param xs Either a peer ID, an actor handle or a network info.
  virtual void cannot_remove_peer(const endpoint_id& x);

  /// Called whenever the user tried to unpeer from an unknown peer.
  /// @param xs Either a peer ID, an actor handle or a network info.
  virtual void cannot_remove_peer(const network_info& x);

  /// Called whenever establishing a connection to a remote peer failed.
  /// @param xs Either a peer ID or a network info.
  virtual void peer_unavailable(const network_info& x);

  /// Called when the @ref endpoint signals system shutdown.
  virtual void shutdown(shutdown_options options);

  // -- initialization ---------------------------------------------------------

  /// Creates the default behavior for the actor that remains valid until the
  /// system is shutting down.
  virtual caf::behavior make_behavior();

protected:
  // -- implementation details -------------------------------------------------

  void cleanup(const endpoint_id& peer_id);

  std::vector<endpoint_id> extract_new_peers(const endpoint_id_list& path) {
    std::vector<endpoint_id> result;
    auto is_new = [this](const auto& id) { return !reachable(tbl_, id); };
    for (const auto& id : path)
      if (is_new(id))
        result.emplace_back(id);
    return result;
  }

  // -- member variables -------------------------------------------------------

  /// Points to the actor owning this object.
  caf::event_based_actor* self_;

  /// Identifies this peer in the network.
  endpoint_id id_;

  /// Stores routing information for reaching other peers. The *transport* adds
  /// new entries to this table (before calling ::peer_connected) and the peer
  /// removes entries in its ::peer_disconnected callback implementation.
  routing_table tbl_;

  /// A logical timestamp.
  lamport_timestamp timestamp_;

  /// Stores prefixes with subscribers on this peer.
  shared_filter_ptr filter_;

  /// Convenience type for annotating a filter with a Lamport time.
  struct versioned_filter {
    /// Time stamp of the peer for the last filter update message. We keep track
    /// of this time stamp  separately, because the time stamp in the routing
    /// table may be more up-to-date than this one. However, we must still apply
    /// filter updates if they are newer than the last write even if we received
    /// a flooded message with a newer time stamp before the filter update.
    lamport_timestamp ts;

    /// The actual content of the filter.
    filter_type entry;
  };

  /// Stores peers that were discovered but did not provide a filter yet. These
  /// are currently hidden to the user because we cannot reasonably interacting
  /// with this peer yet.
  std::vector<endpoint_id> hidden_peers_;

  /// Stores all filters from other peers.
  std::unordered_map<endpoint_id, versioned_filter, detail::fnv> peer_filters_;

  /// Stores revoked paths.
  revocations_type revocations_;

  /// Stores whether this peer disabled forwarding, i.e., only appears as leaf
  /// node to other peers.
  bool disable_forwarding_ = false;

  /// Stores IDs of peers that we have no path to yet but some local actor is
  /// arleady waiting for. Usually for testing purposes.
  std::multimap<endpoint_id, caf::response_promise> awaited_peers_;
};

} // namespace broker::alm
