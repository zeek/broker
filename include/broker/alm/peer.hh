#pragma once

#include <map>
#include <vector>

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/behavior.hpp>
#include <caf/local_actor.hpp>

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
///   void send(const CommunicationHandle& receiver, Ts&&... xs);
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
/// (atom::get, atom::id) -> peer_id_type
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
/// (atom::subscribe, peer_id_list path, filter_type filter, lamport_timestamp)
/// -> void
/// => handle_filter_update(path, filter, t)
///
/// (atom::revoke, revoker, ts, hop) -> void
/// => handle_path_revocation(revoker, ts, hop)
///
/// (atom::await, PeerId) -> void
/// => await_endpoint()
/// ~~~
template <class Derived, class PeerId, class CommunicationHandle>
class peer {
public:
  // -- member types -----------------------------------------------------------

  using routing_table_type = routing_table<PeerId, CommunicationHandle>;

  using peer_id_type = PeerId;

  using peer_id_list = std::vector<peer_id_type>;

  using communication_handle_type = CommunicationHandle;

  using message_type = generic_node_message<peer_id_type>;

  using multipath_type = multipath<peer_id_type>;

  // -- nested types -----------------------------------------------------------

  struct blacklist_type {
    alm::blacklist<peer_id_type> entries;

    caf::timespan aging_interval;

    caf::timespan max_age;

    caf::actor_clock::time_point next_aging_cycle;
  };

  // -- constructors, destructors, and assignment operators --------------------

  peer() {
    blacklist_.aging_interval = defaults::path_blacklist::aging_interval;
    blacklist_.max_age = defaults::path_blacklist::max_age;
    blacklist_.next_aging_cycle = caf::actor_clock::time_point{};
  }

  explicit peer(caf::local_actor* selfptr) {
    using caf::get_or;
    auto& cfg = selfptr->system().config();
    disable_forwarding_ = get_or(cfg, "broker.disable-forwarding", false);
    namespace pb = broker::defaults::path_blacklist;
    blacklist_.aging_interval
      = get_or(cfg, "broker.path-blacklist.aging-interval", pb::aging_interval);
    blacklist_.max_age
      = get_or(cfg, "broker.path-blacklist.max-age", pb::max_age);
    blacklist_.next_aging_cycle
      = selfptr->clock().now() + blacklist_.aging_interval;
  }

  // -- properties -------------------------------------------------------------

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

  auto peer_filter(const peer_id_type& x) const {
    auto i = peer_filters_.find(x);
    if (i != peer_filters_.end())
      return i->second;
    return filter_type{};
  }

  auto timestamp() const noexcept {
    return timestamp_;
  }

  auto peer_handles() const {
    std::vector<communication_handle_type> result;
    for (auto& kvp : tbl_)
      result.emplace_back(kvp.second.hdl);
    return result;
  }

  auto peer_ids() const {
    peer_id_list result;
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

  bool has_remote_subscriber(const topic& x) const noexcept {
    detail::prefix_matcher matches;
    for (const auto& [peer, filter] : peer_filters_)
      if (matches(filter, x))
        return true;
    return false;
  }

  static bool contains(const std::vector<peer_id_type>& ids,
                       const peer_id_type& id) {
    auto predicate = [&](const peer_id_type& pid) { return pid == id; };
    return std::any_of(ids.begin(), ids.end(), predicate);
  }

  // -- flooding ---------------------------------------------------------------

  /// Floods the subscriptions on this peer to all other peers.
  /// @note The functions *does not* bump the Lamport timestamp before sending.
  void flood_subscriptions() {
    peer_id_list path{dref().id()};
    vector_timestamp ts{timestamp_};
    for_each_direct(tbl_, [&](auto&, auto& hdl) {
      dref().send(hdl, atom::subscribe_v, path, ts, filter_);
    });
  }

  /// Floods a path revocation to all other peers.
  /// @note The functions does *not* bump the Lamport timestamp before sending.
  void flood_path_revocation(const peer_id_type& lost_peer) {
    // We bundle path revocation and subscription flooding, because other peers
    // in the network could drop in-flight subscription updates after seeing a
    // newer timestamp with the path revocation.
    peer_id_list path{dref().id()};
    vector_timestamp ts{timestamp_};
    for_each_direct(tbl_, [&, this](const auto& id, const auto& hdl) {
      dref().send(hdl, atom::revoke_v, path, ts, lost_peer, filter_);
    });
  }

  // -- publish and subscribe functions ----------------------------------------

  void subscribe(const filter_type& what) {
    BROKER_TRACE(BROKER_ARG(what));
    auto not_internal = [](const topic& x) { return !is_internal(x); };
    if (!filter_extend(filter_, what, not_internal)) {
      BROKER_DEBUG("already subscribed to topic");
      return;
    }
    ++timestamp_;
    flood_subscriptions();
  }

  template <class T>
  void publish(const T& content) {
    BROKER_TRACE(BROKER_ARG(content));
    const auto& topic = get_topic(content);
    detail::prefix_matcher matches;
    peer_id_list receivers;
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

  void publish_command_to(command_message& content, const peer_id_type& dst) {
    BROKER_TRACE(BROKER_ARG(content) << BROKER_ARG(dst));
    if (dst == dref().id()) {
      dref().publish_locally(content);
      return;
    }
    ship(content, dst);
  }

  /// Checks whether a path and its associated vector timestamp are non-empty,
  /// loop-free and not blacklisted.
  bool valid (peer_id_list& path, vector_timestamp path_ts) {
    // Drop if empty or if path and path_ts have different sizes.
    if (path.empty()) {
      BROKER_WARNING("drop message: path empty");
      return false;
    }
    if (path.size() != path_ts.size()) {
      BROKER_WARNING("drop message: path and timestamp have different sizes");
      return false;
    }
    // Sanity check: we can only receive messages from direct connections.
    auto forwarder = find_row(tbl_, path.back());
    if (forwarder == nullptr) {
      BROKER_WARNING("received message from an unrecognized peer");
      return false;
    }
    if (!forwarder->hdl) {
      BROKER_WARNING("received message from a peer we don't have a direct "
                     " connection to");
      return false;
    }
    // Drop all paths that contain loops.
    if (contains(path, dref().id())) {
      BROKER_DEBUG("drop message: path contains a loop");
      return false;
    }
    // Drop all messages that arrive after blacklisting a path.
    if (blacklisted(path, path_ts, blacklist_.entries)) {
      BROKER_DEBUG("drop message from a blacklisted path");
      return false;
    }
    return true;
  }

  /// Removes all entries from the blacklist that exceeded their maximum age.
  /// The purpose of this function is to clean up state periodically to avoid
  /// unbound growth of the blacklist. Calling it at fixed intervals is not
  /// required. Triggering aging on peer messages suffices, since only peer
  /// messages can grow the blacklist in the first place.
  void age_blacklist() {
    if (blacklist_.entries.empty())
      return;
    auto now = dref().self()->clock().now();
    if (now < blacklist_.next_aging_cycle)
      return;
    auto predicate = [this, now](const auto& entry) {
      return entry.first_seen + blacklist_.max_age <= now;
    };
    auto& entries = blacklist_.entries;
    entries.erase(std::remove_if(entries.begin(), entries.end(), predicate),
                  entries.end());
    blacklist_.next_aging_cycle = now + blacklist_.aging_interval;
  }

  /// Adds the reverse `path` to the routing table and stores the subscription
  /// if it is new.
  /// @returns A pair containing a list of new peers learned through the update
  ///          and a boolean that is set to `true` if this update increased the
  ///          local time.
  /// @note increases the local time if the routing table changes.
  std::pair<std::vector<peer_id_type>, bool>
  handle_update(peer_id_list& path, vector_timestamp path_ts,
                const filter_type& filter) {
    BROKER_TRACE(BROKER_ARG(path) << BROKER_ARG(path_ts) << BROKER_ARG(filter));
    std::vector<peer_id_type> new_peers;
    // Extract new peers from the path.
    auto is_new = [this](const auto& id) { return !reachable(tbl_, id); };
    for (const auto& id : path)
      if (is_new(id))
        new_peers.emplace_back(id);
    // Update the routing table.
    auto added_tbl_entry = add_or_update_path(
      tbl_, path[0], peer_id_list{path.rbegin(), path.rend()},
      vector_timestamp{path_ts.rbegin(), path_ts.rend()});
    BROKER_ASSERT(new_peers.empty() || added_tbl_entry);
    // Increase local time, but only if we have changed the routing table.
    // Otherwise, we would cause infinite flooding, because the peers would
    // never agree on a vector time.
    if (added_tbl_entry) {
      BROKER_DEBUG("increase local time");
      ++timestamp_;
    }
    // Store the subscription if it's new.
    const auto& subscriber = path[0];
    peer_timestamps_[subscriber] = path_ts[0];
    peer_filters_[subscriber] = filter;
    // Trigger await callbacks if necessary.
    if (auto [first, last] = awaited_peers_.equal_range(subscriber);
        first != last) {
      std::for_each(first, last, [&subscriber](auto& kvp) {
        kvp.second.deliver(subscriber);
      });
      awaited_peers_.erase(first, last);
    }
    return {std::move(new_peers), added_tbl_entry};
  }

  void handle_filter_update(peer_id_list& path, vector_timestamp path_ts,
                            const filter_type& filter) {
    BROKER_TRACE(BROKER_ARG(path) << BROKER_ARG(path_ts) << BROKER_ARG(filter));
    // Handle message content (drop nonsense messages and blacklisted paths).
    if (!valid(path, path_ts))
      return;
    auto new_peers = std::move(handle_update(path, path_ts, filter).first);
    // Forward message to all other neighbors.
    if (!disable_forwarding_) {
      path.emplace_back(dref().id());
      path_ts.emplace_back(timestamp_);
      for_each_direct(tbl_, [&](auto& pid, auto& hdl) {
        if (!contains(path, pid))
          dref().send(hdl, atom::subscribe_v, path, path_ts, filter);
      });
    }
    // If we have learned new peers, we flood our own subscriptions as well.
    if (!new_peers.empty()) {
      BROKER_DEBUG("learned new peers: " << new_peers);
      for (auto& id : new_peers)
        dref().peer_discovered(id);
      // TODO: This primarly makes sure that eventually all peers know each
      //       other. There may be more efficient ways to ensure connectivity,
      //       though.
      flood_subscriptions();
    }
    // Clean up some state if possible.
    age_blacklist();
  }

  void handle_path_revocation(peer_id_list& path, vector_timestamp path_ts,
                              const peer_id_type& revoked_hop,
                              const filter_type& filter) {
    BROKER_TRACE(BROKER_ARG(path)
                 << BROKER_ARG(path_ts) << BROKER_ARG(revoked_hop)
                 << BROKER_ARG(filter));
    // Drop nonsense messages.
    if (!valid(path, path_ts))
      return;
    // Handle the subscription part of the message.
    auto&& [new_peers, increased_time] = handle_update(path, path_ts, filter);
    // Handle the recovation part of the message.
    auto [i, added] = emplace(blacklist_.entries, dref().self(), path[0],
                              path_ts[0], revoked_hop);
    if (added) {
      if (!increased_time)
        ++timestamp_;
      auto on_drop = [this](const peer_id_type& whom) {
        BROKER_INFO("lost peer " << whom << " as a result of path revocation");
        dref().peer_unreachable(whom);
      };
      revoke(tbl_, *i, on_drop);
    }
    // Forward message to all other neighbors.
    if (!disable_forwarding_) {
      path.emplace_back(dref().id());
      path_ts.emplace_back(timestamp_);
      for_each_direct(tbl_, [&](auto& pid, auto& hdl) {
        if (!contains(path, pid))
          dref().send(hdl, atom::revoke_v, path, path_ts, revoked_hop, filter);
      });
    }
    // If we have learned new peers, we flood our own subscriptions as well.
    if (!new_peers.empty()) {
      BROKER_DEBUG("learned new peers: " << new_peers);
      for (auto& id : new_peers)
        dref().peer_discovered(id);
      flood_subscriptions();
    }
    // Clean up some state if possible.
    age_blacklist();
  }

  void handle_publication(message_type& msg) {
    // Verify that we are supposed to handle this message.
    auto& path = get_unshared_path(msg);
    if (path.head() != dref().id()) {
      BROKER_WARNING("Received a message for a different node: drop.");
      return;
    }
    // Dispatch locally if we are on the list of receivers.
    auto& receivers = get_unshared_receivers(msg);
    auto i = std::remove(receivers.begin(), receivers.end(), dref().id());
    if (i != receivers.end()) {
      receivers.erase(i, receivers.end());
      if (is_data_message(msg))
        dref().ship_locally(get_data_message(msg));
      else
        dref().ship_locally(get_command_message(msg));
    }
    if (receivers.empty()) {
      if (!path.nodes().empty())
        BROKER_WARNING("More nodes in path but list of receivers is empty.");
      return;
    }
    if (disable_forwarding_) {
      BROKER_WARNING("Asked to forward a message, but forwarding is disabled.");
      return;
    }
    for (auto& node : path.nodes()) {
      message_type nmsg{caf::get<0>(msg), std::move(node), receivers};
      ship(nmsg);
    }
  }

  /// Forwards `msg` to all receivers.
  void ship(message_type& msg) {
    BROKER_TRACE(BROKER_ARG(msg));
    const auto& path = get_path(msg);
    if (auto i = tbl_.find(path.head()); i != tbl_.end()) {
      dref().send(i->second.hdl, atom::publish_v, std::move(msg));
    } else {
      BROKER_WARNING("cannot ship message: no path found to" << path.head());
    }
  }

  /// Forwards `data_msg` to a single `receiver`.
  template <class T>
  void ship(T& msg, const peer_id_type& receiver) {
    BROKER_TRACE(BROKER_ARG(msg) << BROKER_ARG(receiver));
    if (auto ptr = shortest_path(tbl_, receiver)) {
      message_type wrapped{std::move(msg),
                           multipath_type{ptr->begin(), ptr->end()},
                           peer_id_list{receiver}};
      ship(wrapped);
    } else {
      BROKER_WARNING("cannot ship message: no path found to" << receiver);
    }
  }

  /// Forwards `data_msg` to all `receivers`.
  template <class T>
  void ship(T& msg, const peer_id_list& receivers) {
    BROKER_TRACE(BROKER_ARG(msg) << BROKER_ARG(receivers));
    std::vector<multipath_type> paths;
    std::vector<peer_id_type> unreachables;
    generate_paths(receivers, tbl_, paths, unreachables);
    for (auto& path : paths) {
      message_type wrapped{msg, std::move(path), receivers};
      ship(wrapped);
    }
    if (!unreachables.empty())
      BROKER_WARNING("no paths to " << unreachables);
  }

  // -- callbacks --------------------------------------------------------------

  /// Called whenever new data for local subscribers became available.
  /// @param msg Data or command message, either received by peers or generated
  ///            from a local publisher.
  /// @tparam T Either ::data_message or ::command_message.
  template <class T>
  void ship_locally([[maybe_unused]] const T& msg) {
    // nop
  }

  /// Called whenever this peer discovers a new peer in the network.
  /// @param peer_id ID of the new peer.
  /// @note The new peer gets stored in the routing table *before* calling this
  ///       member function.
  void peer_discovered([[maybe_unused]] const peer_id_type& peer_id) {
    // nop
  }

  /// Called whenever this peer established a new connection.
  /// @param peer_id ID of the newly connected peer.
  /// @param hdl Communication handle for exchanging messages with the new peer.
  ///            The handle is default-constructed if no direct connection
  ///            exists (yet).
  /// @note The new peer gets stored in the routing table *before* calling this
  ///       member function.
  void peer_connected([[maybe_unused]] const peer_id_type& peer_id,
                      [[maybe_unused]] const communication_handle_type& hdl) {
    // nop
  }

  /// Called whenever this peer lost a connection to a remote peer.
  /// @param peer_id ID of the disconnected peer.
  /// @param hdl Communication handle of the disconnected peer.
  /// @param reason None if we closed the connection gracefully, otherwise
  ///               contains the transport-specific error code.
  void peer_disconnected([[maybe_unused]] const peer_id_type& peer_id,
                         [[maybe_unused]] const communication_handle_type& hdl,
                         [[maybe_unused]] const error& reason) {
    BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG(hdl) << BROKER_ARG(reason));
    // Do the same cleanup steps we do for removed peers. We intentionally do
    // *not* dispatch through dref() to not trigger undesired side effects.
    peer_removed(peer_id, hdl);
  }

  /// Called whenever this peer removed a direct connection to a remote peer.
  /// @param peer_id ID of the removed peer.
  /// @param hdl Communication handle of the removed peer.
  void peer_removed([[maybe_unused]] const peer_id_type& peer_id,
                    [[maybe_unused]] const communication_handle_type& hdl) {
    BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG(hdl));
    auto on_drop = [this](const peer_id_type& whom) {
      dref().peer_unreachable(whom);
    };
    if (!erase_direct(tbl_, peer_id, on_drop))
      return;
    ++timestamp_;
    flood_path_revocation(peer_id);
  }

  /// Called after removing the last path to `peer_id` from the routing table.
  /// @param peer_id ID of the (now unreachable) peer.
  void peer_unreachable(const peer_id_type& peer_id) {
    peer_filters_.erase(peer_id);
  }

  /// Called whenever the user tried to unpeer from an unknown peer.
  /// @param xs Either a peer ID, an actor handle or a network info.
  template <class T>
  void cannot_remove_peer([[maybe_unused]] const T& x) {
    BROKER_DEBUG("cannot unpeer from uknown peer" << x);
  }

  /// Called whenever establishing a connection to a remote peer failed.
  /// @param xs Either a peer ID or a network info.
  template <class T>
  void peer_unavailable([[maybe_unused]] const T& x) {
    // nop
  }

  /// Called when the @ref endpoint signals system shutdown.
  void shutdown([[maybe_unused]] shutdown_options options) {
    BROKER_TRACE(BROKER_ARG(options));
    BROKER_DEBUG("cancel any pending await_peer requests");
    auto cancel = make_error(ec::shutting_down);
    if (!awaited_peers_.empty()) {
      for (auto& kvp : awaited_peers_)
        kvp.second.deliver(cancel);
      awaited_peers_.clear();
    }
    if (!disable_forwarding_) {
      BROKER_DEBUG("revoke all paths through this peer");
      ++timestamp_;
      auto ids = peer_ids();
      for (auto& x : ids)
        flood_path_revocation(x);
    }
    dref().self()->quit();
  }

  // -- factories --------------------------------------------------------------

  /// Creates the default behavior for the actor that remains valid until the
  /// system is shutting down.
  template <class... Fs>
  caf::behavior make_behavior(Fs... fs) {
    using detail::lift;
    auto& d = dref();
    return {
      std::move(fs)...,
      lift<atom::publish>(d, &Derived::publish_data),
      lift<atom::publish>(d, &Derived::publish_command),
      lift<atom::publish>(d, &Derived::publish_command_to),
      lift<atom::subscribe>(d, &Derived::subscribe),
      lift<atom::publish>(d, &Derived::handle_publication),
      lift<atom::subscribe>(d, &Derived::handle_filter_update),
      lift<atom::revoke>(d, &Derived::handle_path_revocation),
      [=](atom::get, atom::id) { return dref().id(); },
      [=](atom::get, atom::peer, atom::subscriptions) {
        // For backwards-compatibility, we only report the filter of our
        // direct peers. Returning all filter would make more sense in an
        // ALM setting, but that would change the semantics of
        // endpoint::peer_filter.
        auto is_direct_peer = [this](const auto& peer_id) {
          return tbl_.count(peer_id) != 0;
        };
        filter_type result;
        for (const auto& [peer, filter] : peer_filters_)
          if (is_direct_peer(peer))
            filter_extend(result, filter);
        return result;
      },
      [=](atom::shutdown, shutdown_options opts) {
        dref().shutdown(opts);
      },
      [=](atom::publish, atom::local, command_message& msg) {
        dref().ship_locally(msg);
      },
      [=](atom::publish, atom::local, data_message& msg) {
        dref().ship_locally(msg);
      },
      [=](atom::await, peer_id_type who) {
        auto rp = dref().self()->make_response_promise();
        if (auto i = peer_filters_.find(who); i != peer_filters_.end())
          rp.deliver(who);
        else
          awaited_peers_.emplace(who, std::move(rp));
      },
    };
  }

private:
  Derived& dref() {
    return static_cast<Derived&>(*this);
  }

  /// Stores routing information for reaching other peers. The *transport* adds
  /// new entries to this table (before calling ::peer_connected) and the peer
  /// removes entries in its ::peer_disconnected callback implementation.
  routing_table_type tbl_;

  /// A logical timestamp.
  lamport_timestamp timestamp_;

  /// Keeps track of the logical timestamps last seen from other peers.
  std::unordered_map<peer_id_type, lamport_timestamp> peer_timestamps_;

  /// Stores prefixes with subscribers on this peer.
  filter_type filter_;

  /// Stores all filters from other peers.
  std::unordered_map<peer_id_type, filter_type> peer_filters_;

  /// Stores revoked paths.
  blacklist_type blacklist_;

  /// Stores whether this peer disabled forwarding, i.e., only appears as leaf
  /// node to other peers.
  bool disable_forwarding_ = false;

  /// Stores IDs of peers that we have no path to yet but some local actor is
  /// arleady waiting for. Usually for testing purposes.
  std::multimap<peer_id_type, caf::response_promise> awaited_peers_;
};

} // namespace broker::alm
