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
      return i->second;
    return filter_type{};
  }

  auto timestamp() const noexcept {
    return timestamp_;
  }

  auto peer_handles() const {
    std::vector<caf::actor> result;
    for (auto& kvp : tbl_)
      result.emplace_back(kvp.second.hdl);
    return result;
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
  bool dispatch_to_impl(T&& msg, endpoint_id&& receiver);

  /// Dispatches `msg` to `receiver`, ignoring subscription filters.
  /// @returns `true` on success, `false` if no path to the receiver exists.
  bool dispatch_to(data_message msg, endpoint_id receiver);

  /// Dispatches `msg` to `receiver`, ignoring subscription filters.
  /// @returns `true` on success, `false` if no path to the receiver exists.
  bool dispatch_to(command_message msg, endpoint_id receiver);

  // -- flooding ---------------------------------------------------------------

  /// Floods the subscriptions on this peer to all other peers.
  /// @note The functions *does not* bump the Lamport timestamp before sending.
  void flood_subscriptions();

  /// Floods a path revocation to all other peers.
  /// @note The functions does *not* bump the Lamport timestamp before sending.
  void flood_path_revocation(const endpoint_id& lost_peer);

  /// Checks whether a path and its associated vector timestamp are non-empty,
  /// loop-free and not revoked.
  bool valid(endpoint_id_list& path, vector_timestamp path_ts);

  /// Removes all entries from the revocations that exceeded their maximum age.
  /// The purpose of this function is to clean up state periodically to avoid
  /// unbound growth of the revocations. Calling it at fixed intervals is not
  /// required. Triggering aging on peer messages suffices, since only peer
  /// messages can grow the revocations in the first place.
  void age_revocations();

  /// Adds the reverse `path` to the routing table and stores the subscription
  /// if it is new.
  /// @returns A pair containing a list of new peers learned through the update
  ///          and a boolean that is set to `true` if this update increased the
  ///          local time.
  /// @note increases the local time if the routing table changes.
  std::pair<endpoint_id_list, bool> handle_update(endpoint_id_list& path,
                                                  vector_timestamp path_ts,
                                                  const filter_type& filter);

  virtual void handle_filter_update(endpoint_id_list& path,
                                    vector_timestamp& path_ts,
                                    const filter_type& filter);

  virtual void handle_path_revocation(endpoint_id_list& path,
                                      vector_timestamp& path_ts,
                                      const endpoint_id& revoked_hop,
                                      const filter_type& filter);

  // -- interface to the transport ---------------------------------------------

  /// Publishes (floods) a subscription update to @p dst.
  /// @param dst Destination (receiver) for the published data.
  /// @param path Lists Broker peers that forwarded the subscription on the
  ///             overlay in chronological order.
  /// @param ts Stores the logical time of each peer at the time of forwarding.
  /// @param new_filter The new filter to apply to the origin of the update.
  virtual void publish(const caf::actor& dst, atom::subscribe,
                       const endpoint_id_list& path, const vector_timestamp& ts,
                       const filter_type& new_filter)
    = 0;

  /// Publishes (floods) a path revocation update to @p dst.
  /// @param dst Destination (receiver) for the published data.
  /// @param path Lists Broker peers that forwarded the subscription on the
  ///             overlay in chronological order.
  /// @param ts Stores the logical time of each peer at the time of forwarding.
  /// @param lost_peer ID of the affected peer, i.e., the origin of the update
  ///                  lost its communication path to @p lost_peer.
  /// @param new_filter The new filter to apply to the origin of the update.
  virtual void publish(const caf::actor& dst, atom::revoke,
                       const endpoint_id_list& path, const vector_timestamp& ts,
                       const endpoint_id& lost_peer,
                       const filter_type& new_filter)
    = 0;

  /// Publishes @p msg to all local subscribers.
  /// @param msg The published data.
  virtual void publish_locally(const data_message& msg) = 0;

  /// Publishes @p msg to all local subscribers.
  /// @param msg The published command.
  virtual void publish_locally(const command_message& msg) = 0;

  virtual void dispatch(const data_message& msg) = 0;

  virtual void dispatch(const command_message& msg) = 0;

  virtual void dispatch(const node_message& msg) = 0;

  // -- callbacks --------------------------------------------------------------

  /// Called whenever this peer discovers a new peer in the network.
  /// @param peer_id ID of the new peer.
  /// @note The new peer gets stored in the routing table *before* calling this
  ///       member function.
  virtual void peer_discovered(const endpoint_id& peer_id);

  /// Called whenever this peer established a new connection.
  /// @param peer_id ID of the newly connected peer.
  /// @param hdl Communication handle for exchanging messages with the new peer.
  ///            The handle is default-constructed if no direct connection
  ///            exists (yet).
  /// @note The new peer gets stored in the routing table *before* calling this
  ///       member function.
  virtual void peer_connected(const endpoint_id& peer_id,
                              const caf::actor& hdl);

  /// Called whenever this peer lost a connection to a remote peer.
  /// @param peer_id ID of the disconnected peer.
  /// @param hdl Communication handle of the disconnected peer.
  /// @param reason None if we closed the connection gracefully, otherwise
  ///               contains the transport-specific error code.
  virtual void peer_disconnected(const endpoint_id& peer_id,
                                 const caf::actor& hdl, const error& reason);

  /// Called whenever this peer removed a direct connection to a remote peer.
  /// @param peer_id ID of the removed peer.
  /// @param hdl Communication handle of the removed peer.
  virtual void peer_removed(const endpoint_id& peer_id, const caf::actor& hdl);

  /// Called after removing the last path to `peer_id` from the routing table.
  /// @param peer_id ID of the (now unreachable) peer.
  virtual void peer_unreachable(const endpoint_id& peer_id);

  /// Called whenever the user tried to unpeer from an unknown peer.
  /// @param xs Either a peer ID, an actor handle or a network info.
  virtual void cannot_remove_peer(const endpoint_id& x);

  /// Called whenever the user tried to unpeer from an unknown peer.
  /// @param xs Either a peer ID, an actor handle or a network info.
  virtual void cannot_remove_peer(const caf::actor& x);

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

  void cleanup(const endpoint_id& peer_id, const caf::actor& hdl);

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

  /// Keeps track of the logical timestamps last seen from other peers.
  std::unordered_map<endpoint_id, lamport_timestamp, detail::fnv>
    peer_timestamps_;

  /// Stores prefixes with subscribers on this peer.
  shared_filter_ptr filter_;

  /// Stores all filters from other peers.
  std::unordered_map<endpoint_id, filter_type, detail::fnv> peer_filters_;

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
