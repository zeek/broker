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
#include "broker/detail/lift.hh"
#include "broker/detail/prefix_matcher.hh"
#include "broker/error.hh"
#include "broker/filter_type.hh"
#include "broker/logger.hh"
#include "broker/message.hh"
#include "broker/network_info.hh"
#include "broker/shutdown_options.hh"

namespace broker::alm {

/// CRTP base class that represents a Broker peer in the network. This class
/// implements subscription and path management for the overlay. Data transport
/// as well as shipping data to local subscribers is implemented by `Derived`.
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
  // -- member types -----------------------------------------------------------

  using routing_table_type = routing_table<endpoint_id, caf::actor>;

  using multipath_type = multipath<endpoint_id>;

  // -- constants --------------------------------------------------------------

  static constexpr bool has_nothrow_assignable_id
    = std::is_nothrow_move_assignable<endpoint_id>::value;

  // -- nested types -----------------------------------------------------------

  struct blacklist_type {
    alm::blacklist<endpoint_id> entries;

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

  auto& blacklist() const noexcept {
    return blacklist_;
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

  // -- flooding ---------------------------------------------------------------

  /// Floods the subscriptions on this peer to all other peers.
  /// @note The functions *does not* bump the Lamport timestamp before sending.
  void flood_subscriptions();

  /// Floods a path revocation to all other peers.
  /// @note The functions does *not* bump the Lamport timestamp before sending.
  void flood_path_revocation(const endpoint_id& lost_peer);

  // -- publish and subscribe functions ----------------------------------------

  /// Adds a new topic to the local filter and floods the new topic filter to
  /// peers on changes.
  virtual void subscribe(const filter_type& what);

  // Note: publish isn't virtual, because the customization point is `send`.
  template <class T>
  void publish(const T& content) {
    BROKER_TRACE(BROKER_ARG(content));
    const auto& topic = get_topic(content);
    detail::prefix_matcher matches;
    endpoint_id_list receivers;
    for (const auto& [peer, filter] : peer_filters_)
      if (matches(filter, topic))
        receivers.emplace_back(peer);
    if (receivers.empty()) {
      BROKER_DEBUG("drop message: no subscribers found for topic" << topic);
      return;
    }
    ship(content, receivers);
  }

  void publish(node_message_content& content) {
    if (is_data_message(content))
      publish(get<data_message>(content));
    else
      publish(get<command_message>(content));
  }

  void publish_data(data_message& content) {
    publish(content);
  }

  void publish_command(command_message& content) {
    publish(content);
  }

  void publish_command_to(command_message& content, const endpoint_id& dst) {
    BROKER_TRACE(BROKER_ARG(content) << BROKER_ARG(dst));
    if (dst == id_)
      ship_locally(content);
    else
      ship(content, dst);
  }

  /// Checks whether a path and its associated vector timestamp are non-empty,
  /// loop-free and not blacklisted.
  bool valid(endpoint_id_list& path, vector_timestamp path_ts);

  /// Removes all entries from the blacklist that exceeded their maximum age.
  /// The purpose of this function is to clean up state periodically to avoid
  /// unbound growth of the blacklist. Calling it at fixed intervals is not
  /// required. Triggering aging on peer messages suffices, since only peer
  /// messages can grow the blacklist in the first place.
  void age_blacklist();

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

  virtual void handle_publication(node_message& msg);

  /// Forwards `msg` to a single `receiver`.
  template <class T>
  void ship(T& msg, const endpoint_id& receiver) {
    BROKER_TRACE(BROKER_ARG(msg) << BROKER_ARG(receiver));
    if (auto ptr = shortest_path(tbl_, receiver)) {
      node_message wrapped{std::move(msg),
                           multipath_type{ptr->begin(), ptr->end()},
                           endpoint_id_list{receiver}};
      ship(wrapped);
    } else {
      BROKER_WARNING("cannot ship message: no path found to" << receiver);
    }
  }

  /// Wraps `msg` to a `node_message` and then delegates to the virtual `ship`
  /// overload.
  template <class T>
  void ship(T& msg, const endpoint_id_list& receivers) {
    BROKER_TRACE(BROKER_ARG(msg) << BROKER_ARG(receivers));
    std::vector<multipath_type> paths;
    std::vector<endpoint_id> unreachables;
    generate_paths(receivers, tbl_, paths, unreachables);
    for (auto& path : paths) {
      node_message wrapped{msg, std::move(path), receivers};
      ship(wrapped);
    }
    if (!unreachables.empty())
      BROKER_WARNING("no paths to " << unreachables);
  }

  /// Forwards `msg` to all receivers.
  void ship(node_message& msg) {
    BROKER_TRACE(BROKER_ARG(msg) << BROKER_ARG2("path", get_path(msg)));
    const auto& path = get_path(msg);
    if (auto i = tbl_.find(path.head()); i != tbl_.end()) {
      send(i->second.hdl, atom::publish_v, std::move(msg));
    } else {
      BROKER_WARNING("cannot ship message: no path found to" << path.head());
    }
  }

  // -- transport --------------------------------------------------------------

  /// Publishes `msg` to the peer `receiver`.
  virtual void send(const caf::actor& receiver, atom::publish,
                    node_message content)
    = 0;

  /// Updates subscriptions for this peer by sending a filter update to
  /// `receiver`.
  virtual void send(const caf::actor& receiver, atom::subscribe,
                    const endpoint_id_list& path, const vector_timestamp& ts,
                    const filter_type& new_filter)
    = 0;

  /// Informs `receiver` about a path revocation.
  virtual void send(const caf::actor& receiver, atom::revoke,
                    const endpoint_id_list& path, const vector_timestamp& ts,
                    const endpoint_id& lost_peer, const filter_type& new_filter)
    = 0;

  /// Called whenever new data for local subscribers became available.
  /// @param msg Data or command message, either received by peers or generated
  ///            from a local publisher.
  virtual void ship_locally(const data_message& msg) = 0;

  /// Called whenever new data for local subscribers became available.
  /// @param msg Data or command message, either received by peers or generated
  ///            from a local publisher.
  virtual void ship_locally(const command_message& msg) = 0;

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

  // -- factories --------------------------------------------------------------

  /// Creates the default behavior for the actor that remains valid until the
  /// system is shutting down.
  template <class... Fs>
  caf::behavior make_behavior(Fs... fs) {
    BROKER_DEBUG("make behavior for peer" << id_);
    using detail::lift;
    return {
      std::move(fs)...,
      lift<atom::publish>(*this, &peer::publish_data),
      lift<atom::publish>(*this, &peer::publish_command),
      lift<atom::publish>(*this, &peer::publish_command_to),
      lift<atom::subscribe>(*this, &peer::subscribe),
      lift<atom::publish>(*this, &peer::handle_publication),
      lift<atom::subscribe>(*this, &peer::handle_filter_update),
      lift<atom::revoke>(*this, &peer::handle_path_revocation),
      [=](atom::get, atom::id) { return id_; },
      [=](atom::get, atom::peer, atom::subscriptions) {
        // For backwards-compatibility, we only report the filter of our
        // direct peers. Returning all filter would make more sense in an
        // ALM setting, but that would change the semantics of
        // endpoint::peer_filter.
        auto is_direct_peer
          = [this](const auto& peer_id) { return tbl_.count(peer_id) != 0; };
        filter_type result;
        for (const auto& [peer, filter] : peer_filters_)
          if (is_direct_peer(peer))
            filter_extend(result, filter);
        return result;
      },
      [=](atom::shutdown, shutdown_options opts) { shutdown(opts); },
      [=](atom::publish, atom::local, command_message& msg) {
        ship_locally(msg);
      },
      [=](atom::publish, atom::local, data_message& msg) {
        ship_locally(msg);
      },
      [=](atom::await, endpoint_id who) {
        auto rp = self_->make_response_promise();
        if (auto i = peer_filters_.find(who); i != peer_filters_.end())
          rp.deliver(who);
        else
          awaited_peers_.emplace(who, std::move(rp));
      },
    };
  }

private:
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
  routing_table_type tbl_;

  /// A logical timestamp.
  lamport_timestamp timestamp_;

  /// Keeps track of the logical timestamps last seen from other peers.
  std::unordered_map<endpoint_id, lamport_timestamp> peer_timestamps_;

  /// Stores prefixes with subscribers on this peer.
  filter_type filter_;

  /// Stores all filters from other peers.
  std::unordered_map<endpoint_id, filter_type> peer_filters_;

  /// Stores revoked paths.
  blacklist_type blacklist_;

  /// Stores whether this peer disabled forwarding, i.e., only appears as leaf
  /// node to other peers.
  bool disable_forwarding_ = false;

  /// Stores IDs of peers that we have no path to yet but some local actor is
  /// arleady waiting for. Usually for testing purposes.
  std::multimap<endpoint_id, caf::response_promise> awaited_peers_;
};

} // namespace broker::alm
