#include "broker/alm/peer.hh"

namespace broker::alm {

// -- constructors, destructors, and assignment operators ----------------------

peer::peer(caf::event_based_actor* selfptr) : self_(selfptr) {
  blacklist_.aging_interval = defaults::path_blacklist::aging_interval;
  blacklist_.max_age = defaults::path_blacklist::max_age;
  blacklist_.next_aging_cycle = caf::actor_clock::time_point{};
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

peer::~peer() {
  // nop
}

// -- central_dispatcher overrides ---------------------------------------------

caf::event_based_actor* peer::this_actor() noexcept {
  return self();
}

endpoint_id peer::this_endpoint() const {
  return id();
}

filter_type peer::local_filter() const {
  return filter_;
}

alm::lamport_timestamp peer::local_timestamp() const noexcept {
  return timestamp_;
}

// -- additional dispatch overloads --------------------------------------------

template <class T>
bool peer::dispatch_to_impl(T&& msg, endpoint_id&& receiver) {
  if (auto ptr = shortest_path(tbl_, receiver); ptr && !ptr->empty()) {
    multipath<endpoint_id> path{ptr->begin(), ptr->end()};
    endpoint_id_list receivers;
    receivers.emplace_back(std::move(receiver));
    dispatch(make_node_message(std::forward<T>(msg), std::move(path),
                               std::move(receivers)));
    return true;
  } else {
    BROKER_DEBUG("drop message: no path to" << receiver);
    return false;
  }
}

bool peer::dispatch_to(data_message msg, endpoint_id receiver) {
  BROKER_TRACE(BROKER_ARG(msg) << BROKER_ARG(receiver));
  return dispatch_to_impl(std::move(msg), std::move(receiver));
}

bool peer::dispatch_to(command_message msg, endpoint_id receiver) {
  BROKER_TRACE(BROKER_ARG(msg) << BROKER_ARG(receiver));
  return dispatch_to_impl(std::move(msg), std::move(receiver));
}

// -- convenience functions for subscription information -----------------------

bool peer::has_remote_subscriber(const topic& x) const noexcept {
  detail::prefix_matcher matches;
  for (const auto& [peer, filter] : peer_filters_)
    if (matches(filter, x))
      return true;
  return false;
}

bool peer::contains(const endpoint_id_list& ids, const endpoint_id& id) {
  auto predicate = [&](const endpoint_id& pid) { return pid == id; };
  return std::any_of(ids.begin(), ids.end(), predicate);
}

// -- flooding -----------------------------------------------------------------

void peer::flood_subscriptions() {
  endpoint_id_list path{id_};
  vector_timestamp ts{timestamp_};
  for_each_direct(tbl_, [&](auto&, auto& hdl) {
    publish(hdl, atom::subscribe_v, path, ts, filter_);
  });
}

void peer::flood_path_revocation(const endpoint_id& lost_peer) {
  // We bundle path revocation and subscription flooding, because other peers
  // in the network could drop in-flight subscription updates after seeing a
  // newer timestamp with the path revocation.
  endpoint_id_list path{id_};
  vector_timestamp ts{timestamp_};
  for_each_direct(tbl_, [&, this](const auto& id, const auto& hdl) {
    publish(hdl, atom::revoke_v, path, ts, lost_peer, filter_);
  });
}

// -- publish and subscribe functions ------------------------------------------

void peer::subscribe(const filter_type& what) {
  BROKER_TRACE(BROKER_ARG(what));
  auto not_internal = [](const topic& x) { return !is_internal(x); };
  if (filter_extend(filter_, what, not_internal)) {
    ++timestamp_;
    flood_subscriptions();
  } else {
    BROKER_DEBUG("already subscribed to topic (or topic is internal):" << what);
  }
}

bool peer::valid(endpoint_id_list& path, vector_timestamp path_ts) {
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
    BROKER_WARNING(
      "received message from a peer we don't have a direct connection to");
    return false;
  }
  // Drop all paths that contain loops.
  if (contains(path, id_)) {
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

void peer::age_blacklist() {
  if (blacklist_.entries.empty())
    return;
  auto now = self()->clock().now();
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

std::pair<endpoint_id_list, bool>
peer::handle_update(endpoint_id_list& path, vector_timestamp path_ts,
                    const filter_type& filter) {
  BROKER_TRACE(BROKER_ARG(path) << BROKER_ARG(path_ts) << BROKER_ARG(filter));
  std::vector<endpoint_id> new_peers;
  // Extract new peers from the path.
  auto is_new = [this](const auto& id) { return !reachable(tbl_, id); };
  for (const auto& id : path)
    if (is_new(id))
      new_peers.emplace_back(id);
  // Update the routing table.
  auto added_tbl_entry = add_or_update_path(
    tbl_, path[0], endpoint_id_list{path.rbegin(), path.rend()},
    vector_timestamp{path_ts.rbegin(), path_ts.rend()});
  // Increase local time, but only if we have changed the routing table.
  // Otherwise, we would cause infinite flooding, because the peers would
  // never agree on a vector time.
  if (added_tbl_entry) {
    BROKER_DEBUG("increase local time");
    ++timestamp_;
  }
  // Store the subscription if it's new.
  const auto& subscriber = path[0];
  if (path_ts[0] > peer_timestamps_[subscriber]) {
    peer_timestamps_[subscriber] = path_ts[0];
    peer_filters_[subscriber] = filter;
  }
  // Trigger await callbacks if necessary.
  if (auto [first, last] = awaited_peers_.equal_range(subscriber);
      first != last) {
    std::for_each(first, last,
                  [&subscriber](auto& kvp) { kvp.second.deliver(subscriber); });
    awaited_peers_.erase(first, last);
  }
  return {std::move(new_peers), added_tbl_entry};
}

void peer::handle_filter_update(endpoint_id_list& path,
                                vector_timestamp& path_ts,
                                const filter_type& filter) {
  BROKER_TRACE(BROKER_ARG(path) << BROKER_ARG(path_ts) << BROKER_ARG(filter));
  // Handle message content (drop nonsense messages and blacklisted paths).
  if (!valid(path, path_ts))
    return;
  auto new_peers = std::move(handle_update(path, path_ts, filter).first);
  // Forward message to all other neighbors.
  if (!disable_forwarding_) {
    path.emplace_back(id_);
    path_ts.emplace_back(timestamp_);
    for_each_direct(tbl_, [&](auto& pid, auto& hdl) {
      if (!contains(path, pid))
        publish(hdl, atom::subscribe_v, path, path_ts, filter);
    });
  }
  // If we have learned new peers, we flood our own subscriptions as well.
  if (!new_peers.empty()) {
    BROKER_DEBUG("learned new peers: " << new_peers);
    for (auto& id : new_peers)
      peer_discovered(id);
    // TODO: This primarly makes sure that eventually all peers know each
    //       other. There may be more efficient ways to ensure connectivity,
    //       though.
    flood_subscriptions();
  }
  // Clean up some state if possible.
  age_blacklist();
}

void peer::handle_path_revocation(endpoint_id_list& path,
                                  vector_timestamp& path_ts,
                                  const endpoint_id& revoked_hop,
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
  auto [i, added]
    = emplace(blacklist_.entries, self_, path[0], path_ts[0], revoked_hop);
  if (added) {
    if (!increased_time)
      ++timestamp_;
    auto on_drop = [this](const endpoint_id& whom) {
      BROKER_INFO("lost peer " << whom << " as a result of path revocation");
      peer_unreachable(whom);
    };
    revoke(tbl_, *i, on_drop);
  }
  // Forward message to all other neighbors.
  if (!disable_forwarding_) {
    path.emplace_back(id_);
    path_ts.emplace_back(timestamp_);
    for_each_direct(tbl_, [&](auto& pid, auto& hdl) {
      if (!contains(path, pid))
        publish(hdl, atom::revoke_v, path, path_ts, revoked_hop, filter);
    });
  }
  // If we have learned new peers, we flood our own subscriptions as well.
  if (!new_peers.empty()) {
    BROKER_DEBUG("learned new peers: " << new_peers);
    for (auto& id : new_peers)
      peer_discovered(id);
    flood_subscriptions();
  }
  // Clean up some state if possible.
  age_blacklist();
}

// -- interface to the transport -----------------------------------------------

void peer::publish_locally(const node_message_content& msg) {
  BROKER_TRACE(BROKER_ARG(msg));
  if (is_data_message(msg)) {
    publish_locally(get_data_message(msg));
  } else {
    BROKER_ASSERT(is_command_message(msg));
    publish_locally(get_command_message(msg));
  }
}

// -- callbacks ----------------------------------------------------------------

void peer::peer_discovered(const endpoint_id&) {
  // nop
}

void peer::peer_connected(const endpoint_id&, const caf::actor&) {
  // nop
}

void peer::peer_disconnected(const endpoint_id& peer_id, const caf::actor& hdl,
                             [[maybe_unused]] const error& reason) {
  BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG(hdl) << BROKER_ARG(reason));
  cleanup(peer_id, hdl);
}

void peer::peer_removed([[maybe_unused]] const endpoint_id& peer_id,
                        [[maybe_unused]] const caf::actor& hdl) {
  BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG(hdl));
  cleanup(peer_id, hdl);
}

void peer::peer_unreachable(const endpoint_id& peer_id) {
  peer_filters_.erase(peer_id);
}

void peer::cannot_remove_peer([[maybe_unused]] const endpoint_id& x) {
  BROKER_DEBUG("cannot unpeer from uknown peer" << x);
}

void peer::cannot_remove_peer([[maybe_unused]] const caf::actor& x) {
  BROKER_DEBUG("cannot unpeer from uknown peer" << x);
}

void peer::cannot_remove_peer([[maybe_unused]] const network_info& x) {
  BROKER_DEBUG("cannot unpeer from uknown peer" << x);
}

void peer::peer_unavailable(const network_info&) {
  // nop
}

void peer::shutdown([[maybe_unused]] shutdown_options options) {
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
  self_->quit();
}

// -- initialization -----------------------------------------------------------

caf::behavior peer::make_behavior() {
  BROKER_DEBUG("make behavior for peer" << id_);
  using detail::lift;
  return {
    [this](atom::publish, data_message& msg) {
      dispatch(msg);
    },
    [this](atom::publish, command_message& msg) {
      dispatch(msg);
    },
    [this](atom::publish, command_message& msg, endpoint_id& receiver) {
      if (receiver == id_)
        publish_locally(msg);
      else
        dispatch_to(std::move(msg), std::move(receiver));
    },
    [this](atom::publish, node_message& msg) {
      dispatch(std::move(msg));
    },
    lift<atom::subscribe>(*this, &peer::subscribe),
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
      dispatch(msg);
    },
    [=](atom::publish, atom::local, data_message& msg) {
      dispatch(msg);
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

// -- implementation details ---------------------------------------------------

void peer::cleanup(const endpoint_id& peer_id, const caf::actor& hdl) {
  BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG(hdl));
  auto on_drop = [this](const endpoint_id& whom) { peer_unreachable(whom); };
  if (erase_direct(tbl_, peer_id, on_drop)) {
    ++timestamp_;
    flood_path_revocation(peer_id);
  }
}

} // namespace broker::alm
