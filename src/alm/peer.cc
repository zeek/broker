#include "broker/alm/peer.hh"

#include "broker/endpoint_info.hh"

namespace broker::alm {

// -- constructors, destructors, and assignment operators ----------------------

peer::peer(caf::event_based_actor* selfptr) : self_(selfptr) {
  revocations_.aging_interval = defaults::path_revocations::aging_interval;
  revocations_.max_age = defaults::path_revocations::max_age;
  revocations_.next_aging_cycle = caf::actor_clock::time_point{};
  using caf::get_or;
  auto& cfg = selfptr->system().config();
  disable_forwarding_ = get_or(cfg, "broker.disable-forwarding", false);
  namespace pb = broker::defaults::path_revocations;
  revocations_.aging_interval
    = get_or(cfg, "broker.path-revocations.aging-interval", pb::aging_interval);
  revocations_.max_age
    = get_or(cfg, "broker.path-revocations.max-age", pb::max_age);
  revocations_.next_aging_cycle
    = selfptr->clock().now() + revocations_.aging_interval;
  filter_ = std::make_shared<shared_filter_type>();
}

peer::~peer() {
  // nop
}

// -- additional dispatch overloads --------------------------------------------

template <class T>
bool peer::dispatch_to_impl(T&& msg, endpoint_id receiver) {
  if (auto ptr = shortest_path(tbl_, receiver); ptr && !ptr->empty()) {
    multipath path{ptr->begin(), ptr->end()};
    dispatch(std::move(path), std::forward<T>(msg));
    return true;
  } else {
    BROKER_DEBUG("drop message: no path to" << receiver);
    return false;
  }
}

bool peer::dispatch_to(data_message msg, endpoint_id receiver) {
  BROKER_TRACE(BROKER_ARG(msg) << BROKER_ARG(receiver));
  return dispatch_to_impl(std::move(msg), receiver);
}

bool peer::dispatch_to(command_message msg, endpoint_id receiver) {
  BROKER_TRACE(BROKER_ARG(msg) << BROKER_ARG(receiver));
  return dispatch_to_impl(std::move(msg), receiver);
}

// -- convenience functions for subscription information -----------------------

bool peer::has_remote_subscriber(const topic& x) const noexcept {
  detail::prefix_matcher matches;
  for (const auto& [peer, filter] : peer_filters_)
    if (matches(filter.entry, x))
      return true;
  return false;
}

bool peer::contains(const endpoint_id_list& ids, const endpoint_id& id) {
  auto predicate = [&](const endpoint_id& pid) { return pid == id; };
  return std::any_of(ids.begin(), ids.end(), predicate);
}

// -- publish and subscribe functions ------------------------------------------

void peer::subscribe(const filter_type& what) {
  BROKER_TRACE(BROKER_ARG(what));
  auto changed = filter_->update([this, &what](auto& version, auto& xs) {
    auto not_internal = [](const topic& x) { return !is_internal(x); };
    if (filter_extend(xs, what, not_internal)) {
      version = ++timestamp_;
      return true;
    } else {
      return false;
    }
  });
  // Note: this is the only writer, so we need not worry about the filter
  // changing again concurrently.
  if (changed) {
    publish_filter_update(timestamp_, filter_->read());
  } else {
    BROKER_DEBUG("already subscribed to topics:" << what);
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
  if (!is_direct_connection(*forwarder)) {
    BROKER_WARNING("received a flooded message from peer"
                   << path.back()
                   << "but we don't have a direct connection to it");
    return false;
  }
  // Drop all paths that contain loops.
  if (contains(path, id_)) {
    BROKER_DEBUG("drop message: path contains a loop");
    return false;
  }
  // Drop all messages that arrive after revoking a path.
  if (revoked(path, path_ts, revocations_.entries)) {
    BROKER_DEBUG("drop message from a revoked path");
    return false;
  }
  return true;
}

void peer::age_revocations() {
  if (revocations_.entries.empty())
    return;
  auto now = self()->clock().now();
  if (now < revocations_.next_aging_cycle)
    return;
  auto predicate = [this, now](const auto& entry) {
    return entry.first_seen + revocations_.max_age <= now;
  };
  auto& entries = revocations_.entries;
  entries.erase(std::remove_if(entries.begin(), entries.end(), predicate),
                entries.end());
  revocations_.next_aging_cycle = now + revocations_.aging_interval;
}

void peer::handle_filter_request(const endpoint_id& src) {
  BROKER_TRACE(BROKER_ARG(src));
  publish_filter_update(src, timestamp_, filter_->read());
}

void peer::handle_filter_update(const endpoint_id& src, lamport_timestamp ts,
                                const filter_type& filter) {
  BROKER_TRACE(BROKER_ARG(src) << BROKER_ARG(ts) << BROKER_ARG(filter));
  if (auto i = peer_filters_.find(src); i != peer_filters_.end()) {
    auto& [key, val] = *i;
    // Case 1: override a previous filter with a new version.
    if (val.ts < ts) {
      BROKER_INFO(src << "changes its filter to" << filter);
      val.ts = ts;
      val.entry = std::move(filter);
      ++timestamp_;
      age_revocations();
    } else {
      BROKER_DEBUG("drop outdated filter update for" << src);
    }
  } else if (reachable(tbl_, src)) {
    // Case 2: set the initial filter for the peer. We trigger the
    // peer_discovered event here as opposed to triggering it in
    // `handle_path_discovery` in order to make sure the application is not
    // already sending messages to a peer that did not yet specify its filter.
    BROKER_INFO(src << "sets its filter to" << filter);
    peer_filters_.emplace(src, versioned_filter{ts, filter});
    BROKER_DEBUG(BROKER_ARG(peer_filters_));
    ++timestamp_;
    age_revocations();
    peer_discovered(src);
    // Trigger await callbacks if necessary.
    if (auto [first, last] = awaited_peers_.equal_range(src); first != last) {
      std::for_each(first, last,
                    [&src](auto& kvp) { kvp.second.deliver(src); });
      awaited_peers_.erase(first, last);
    }
  } else {
    // Case 3: stale or obsolete update.
    BROKER_DEBUG("received filter update from unknown peer" << src);
  }
  // We remove a matching entry from hidden_peers_ in all three cases.
  auto& v = hidden_peers_;
  v.erase(std::remove(v.begin(), v.end(), src), v.end());
  // Note: we don't need to forward the message here, because the next hops
  //       picks up the message automatically.
}

void peer::handle_path_discovery(endpoint_id_list& path,
                                 vector_timestamp& path_ts) {
  BROKER_TRACE(BROKER_ARG(path) << BROKER_ARG(path_ts));
  // Drop nonsense messages.
  if (!valid(path, path_ts))
    return;
  auto new_peers = extract_new_peers(path);
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
    age_revocations();
  }
  // Forward message to neighbors.
  if (!disable_forwarding_) {
    path.emplace_back(id_);
    path_ts.emplace_back(timestamp_);
    for_each_direct(tbl_, [&](auto& dst) {
      if (!contains(path, dst))
        forward_path_discovery(dst, path, path_ts);
    });
  }
  // If we have learned new peers, trigger a path discovery on our own. Note
  // that we do not trigger peer_discovered here but wait for the filter update
  // message first. By triggering a path discovery to ourselves, we cause the
  // new peer to send us its filter.
  if (!new_peers.empty()) {
    BROKER_DEBUG("learned new peers: " << new_peers);
    // TODO: This primarily makes sure that eventually all peers know each
    //       other. There may be more efficient ways to ensure connectivity.
    //       A rather simple way to avoid "flooding-chains" would be waiting for
    //       some random time (e.g. 50ms-100ms) before triggering the flooding
    //       message. This way, we can skip some of the flooding messages if
    //       more announcements arrive while we were waiting.
    trigger_path_discovery();
    auto& v = hidden_peers_;
    v.insert(v.end(), new_peers.begin(), new_peers.end());
    std::sort(v.begin(), v.end());
    v.erase(std::unique(v.begin(), v.end()), v.end());
    trigger_filter_requests();
  }
}

void peer::handle_path_revocation(endpoint_id_list& path,
                                  vector_timestamp& path_ts,
                                  const endpoint_id& revoked_hop) {
  BROKER_TRACE(BROKER_ARG(path)
               << BROKER_ARG(path_ts) << BROKER_ARG(revoked_hop));
  // Drop nonsense messages.
  if (!valid(path, path_ts))
    return;
  // Handle the recovation part of the message.
  auto [i, added] = emplace(revocations_.entries, self_, path[0], path_ts[0],
                            revoked_hop);
  if (added) {
    ++timestamp_;
    auto on_drop = [this](const endpoint_id& whom) {
      if (peer_filters_.erase(whom) != 0) {
        BROKER_INFO("lost peer " << whom << " as a result of path revocation");
        peer_unreachable(whom);
      }
    };
    revoke(tbl_, *i, on_drop);
  }
  // Forward message to all other neighbors.
  if (!disable_forwarding_) {
    path.emplace_back(id_);
    path_ts.emplace_back(timestamp_);
    for_each_direct(tbl_, [&](auto& dst) {
      if (!contains(path, dst))
        forward_path_revocation(dst, path, path_ts, revoked_hop);
    });
  }
}

// -- callbacks ----------------------------------------------------------------

void peer::peer_discovered(const endpoint_id&) {
  // nop
}

void peer::peer_connected(const endpoint_id&) {
  // nop
}

void peer::peer_disconnected(const endpoint_id& peer_id,
                             [[maybe_unused]] const error& reason) {
  BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG(reason));
  cleanup(peer_id);
}

void peer::peer_removed([[maybe_unused]] const endpoint_id& peer_id) {
  BROKER_TRACE(BROKER_ARG(peer_id));
  cleanup(peer_id);
}

void peer::peer_unreachable(const endpoint_id& peer_id) {
  peer_filters_.erase(peer_id);
}

void peer::cannot_remove_peer([[maybe_unused]] const endpoint_id& x) {
  BROKER_DEBUG("cannot unpeer from unknown peer" << x);
}

void peer::cannot_remove_peer([[maybe_unused]] const network_info& x) {
  BROKER_DEBUG("cannot unpeer from unknown peer" << x);
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
      trigger_path_revocation(x);
  }
  self_->unbecome();
  self_->set_default_handler(
    [](caf::scheduled_actor* self, caf::message&) -> caf::skippable_result {
      if (self->current_message_id().is_request())
        return caf::make_error(caf::sec::request_receiver_down);
      return caf::make_message();
    });
}

// -- initialization -----------------------------------------------------------

caf::behavior peer::make_behavior() {
  BROKER_DEBUG("make behavior for peer" << id_);
  using detail::lift;
  return {
    [this](atom::publish, const data_message& msg) { //
      dispatch(msg);
    },
    [this](atom::publish, const command_message& msg) { //
      dispatch(msg);
    },
    [this](atom::publish, const endpoint_info& receiver,
           const data_message& msg) {
      dispatch(multipath{receiver.node, true}, msg);
    },
    [this](atom::publish, const command_message& msg, endpoint_id receiver) {
      if (receiver == id_)
        publish_locally(msg);
      else
        dispatch_to(msg, std::move(receiver));
    },
    lift<atom::subscribe>(*this, &peer::subscribe),
    lift<atom::subscribe>(*this, &peer::handle_filter_update),
    lift<atom::revoke>(*this, &peer::handle_path_revocation),
    [this](atom::get, atom::id) { //
      return id_;
    },
    [this](atom::get, atom::peer, atom::subscriptions) {
      // For backwards-compatibility, we only report the filter of our
      // direct peers. Returning all filter would make more sense in an
      // ALM setting, but that would change the semantics of
      // endpoint::peer_filter.
      auto is_direct_peer
        = [this](const auto& peer_id) { return tbl_.count(peer_id) != 0; };
      filter_type result;
      for (const auto& [peer, filter] : peer_filters_)
        if (is_direct_peer(peer))
          filter_extend(result, filter.entry);
      return result;
    },
    [this](atom::shutdown, shutdown_options opts) { //
      shutdown(opts);
    },
    [this](atom::publish, atom::local, command_message& msg) { //
      dispatch(msg);
    },
    [this](atom::publish, atom::local, data_message& msg) { //
      dispatch(msg);
    },
    [this](atom::await, endpoint_id who) {
      auto rp = self_->make_response_promise();
      if (auto i = peer_filters_.find(who); i != peer_filters_.end())
        rp.deliver(who);
      else
        awaited_peers_.emplace(who, std::move(rp));
    },
    [this](atom::get_filter) { //
      return filter_->read();
    },
  };
}

// -- implementation details ---------------------------------------------------

void peer::cleanup(const endpoint_id& peer_id) {
  BROKER_TRACE(BROKER_ARG(peer_id));
  auto on_drop = [this](const endpoint_id& whom) { peer_unreachable(whom); };
  if (erase_direct(tbl_, peer_id, on_drop)) {
    ++timestamp_;
    trigger_path_revocation(peer_id);
  }
}

} // namespace broker::alm
