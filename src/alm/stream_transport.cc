#include "broker/alm/stream_transport.hh"

#include "broker/detail/overload.hh"

namespace broker::alm {

// -- constructors, destructors, and assignment operators ----------------------

stream_transport::stream_transport(caf::event_based_actor* self) : super(self) {
  // nop
}

// -- properties ---------------------------------------------------------------

bool stream_transport::connected_to(const caf::actor& hdl) const noexcept {
  return hdl_to_mgr_.count(hdl) != 0;
}

// -- adding local subscribers -------------------------------------------------

caf::outbound_stream_slot<data_message>
stream_transport::add_sending_worker(filter_type filter) {
  BROKER_TRACE(BROKER_ARG(filter));
  subscribe(filter);
  auto mgr = make_unipath_data_sink(this, std::move(filter));
  auto res = mgr->add_unchecked_outbound_path<data_message>();
  BROKER_ASSERT(res != caf::invalid_stream_slot);
  data_sinks_.emplace_back(std::move(mgr));
  return res;
}

error stream_transport::add_worker(const caf::actor& hdl, filter_type filter) {
  BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(filter));
  if (hdl == nullptr || filter.empty()) {
    return caf::sec::cannot_add_downstream;
  } else {
    subscribe(filter);
    auto mgr = make_unipath_data_sink(this, std::move(filter));
    auto res = mgr->add_unchecked_outbound_path<data_message>(hdl);
    BROKER_ASSERT(res != caf::invalid_stream_slot);
    data_sinks_.emplace_back(std::move(mgr));
    return caf::none;
  }
}

caf::outbound_stream_slot<command_message>
stream_transport::add_sending_store(filter_type filter) {
  BROKER_TRACE(BROKER_ARG(filter));
  subscribe(filter);
  auto mgr = make_unipath_command_sink(this, std::move(filter));
  auto res = mgr->add_unchecked_outbound_path<command_message>();
  BROKER_ASSERT(res != caf::invalid_stream_slot);
  command_sinks_.emplace_back(std::move(mgr));
  return res;
}

error stream_transport::add_store(const caf::actor& hdl, filter_type filter) {
  BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(filter));
  if (hdl == nullptr || filter.empty()) {
    return caf::sec::cannot_add_downstream;
  } else {
    subscribe(filter);
    auto mgr = make_unipath_command_sink(this, std::move(filter));
    auto res = mgr->add_unchecked_outbound_path<command_message>(hdl);
    BROKER_ASSERT(res != caf::invalid_stream_slot);
    command_sinks_.emplace_back(std::move(mgr));
    return caf::none;
  }
}

// -- overrides for peer::publish ----------------------------------------------

void stream_transport::publish(const caf::actor& dst, atom::subscribe,
                               const endpoint_id_list& path,
                               const vector_timestamp& ts,
                               const filter_type& new_filter) {
  BROKER_TRACE(BROKER_ARG(dst)
               << BROKER_ARG(path) << BROKER_ARG(ts) << BROKER_ARG(new_filter));
  self()->send(dst, atom::subscribe_v, path, ts, new_filter);
}

void stream_transport::publish(const caf::actor& dst, atom::revoke,
                               const endpoint_id_list& path,
                               const vector_timestamp& ts,
                               const endpoint_id& lost_peer,
                               const filter_type& new_filter) {
  BROKER_TRACE(BROKER_ARG(dst)
               << BROKER_ARG(path) << BROKER_ARG(ts) << BROKER_ARG(lost_peer)
               << BROKER_ARG(new_filter));
  self()->send(dst, atom::revoke_v, path, ts, lost_peer, new_filter);
}

void stream_transport::publish_locally(const data_message& msg) {
  BROKER_TRACE(BROKER_ARG(msg));
  for (auto& sink : data_sinks_)
    sink->enqueue(msg);
}

void stream_transport::publish_locally(const command_message& msg) {
  BROKER_TRACE(BROKER_ARG(msg));
  for (auto& sink : command_sinks_)
    sink->enqueue(msg);
}

// -- peering ------------------------------------------------------------------

detail::peer_manager_ptr
stream_transport::get_or_insert_pending(const endpoint_id& remote_peer) {
  if (auto i = pending_.find(remote_peer); i != pending_.end()) {
    return i->second;
  } else {
    auto mgr = detail::make_peer_manager(this);
    pending_.emplace(remote_peer, mgr);
    return mgr;
  }
}

detail::peer_manager_ptr stream_transport::get_pending(const caf::actor& hdl) {
  auto pred = [&hdl](const auto& kvp) {
    return kvp.second->handshake().remote_hdl == hdl;
  };
  if (auto i = std::find_if(pending_.begin(), pending_.end(), pred);
      i != pending_.end()) {
    return i->second;
  } else {
    return nullptr;
  }
}

detail::peer_manager_ptr
stream_transport::get_pending(const endpoint_id& remote_peer) {
  if (auto i = pending_.find(remote_peer); i != pending_.end()) {
    return i->second;
  } else {
    return nullptr;
  }
}

// Initiates peering between A (this node) and B (remote peer).
void stream_transport::start_peering(const endpoint_id& remote_peer,
                                     const caf::actor& hdl,
                                     caf::response_promise rp) {
  BROKER_TRACE(BROKER_ARG(remote_peer) << BROKER_ARG(hdl));
  if (is_direct_connection(tbl(), remote_peer)) {
    BROKER_DEBUG("start_peering ignored: already peering with" << remote_peer);
    rp.deliver(atom::peer_v, atom::ok_v, hdl);
  } else if (remote_peer < id()) {
    // We avoid conflicts in the handshake process by always having the node
    // with the smaller ID initiate the peering. Otherwise, we could end up in
    // a deadlock during handshake if both sides send step 1 at the same time.
    if (auto i = pending_.find(remote_peer); i != pending_.end()) {
      auto& mgr = i->second;
      auto& hs = mgr->handshake();
      if (mgr->hdl() != hdl) {
        BROKER_ERROR("multiple peers share a single actor handle!");
        rp.deliver(make_error(ec::invalid_peering_request,
                              "handle already in use by another responder"));
      } else if (!hs.is_responder()) {
        BROKER_ERROR("peer tries to obtained wrong role in handshake!");
        rp.deliver(make_error(ec::invalid_handshake_state));
      } else {
        hs.promises.emplace_back(rp);
      }
    } else {
      auto mgr = make_peer_manager(this);
      pending_.emplace(remote_peer, mgr);
      auto& hs = mgr->handshake();
      hs.to_responder();
      hs.promises.emplace_back(rp);
      auto s = self();
      s->request(hdl, std::chrono::minutes(10), atom::peer_v, id(), s)
        .then(
          [](atom::peer, atom::ok, const endpoint_id&) {
            // nop
          },
          [mgr](caf::error& err) mutable {
            // Abort the handshake if it hasn't started yet. Otherwise, we
            // have other mechanisms in place that capture the same error.
            if (auto& href = mgr->handshake(); !href.started()) {
              BROKER_DEBUG("peering failed:" << err);
              href.fail(std::move(err));
            }
          });
    }
  } else if (auto i = pending_.find(remote_peer); i != pending_.end()) {
    if (i->second->handshake().remote_hdl == hdl) {
      BROKER_DEBUG("start_peering ignored: already started peering with"
                   << remote_peer);
      rp.deliver(atom::peer_v, atom::ok_v, hdl);
    } else {
      BROKER_ERROR("multiple peers share a single actor handle!");
      rp.deliver(make_error(ec::invalid_peering_request,
                            "handle already in use by another responder"));
    }
  } else {
    auto mgr = make_peer_manager(this);
    if (mgr->handshake().originator_start_peering(remote_peer, hdl,
                                                  std::move(rp))) {
      BROKER_DEBUG("start peering with" << remote_peer);
      pending_.emplace(remote_peer, std::move(mgr));
    } else {
      BROKER_ERROR("failed to start peering with" << remote_peer << ":"
                                                  << mgr->handshake().err);
    }
  }
}

caf::outbound_stream_slot<node_message, caf::actor, endpoint_id, filter_type,
                          lamport_timestamp>
stream_transport::handle_peering_request(const endpoint_id& remote_peer,
                                         const caf::actor& hdl) {
  BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(remote_peer));
  if (is_direct_connection(tbl(), remote_peer)) {
    BROKER_ERROR("drop peering request: already have a direct connection to"
                 << remote_peer);
    return {};
  } else if (auto mgr = get_or_insert_pending(remote_peer);
             mgr->handshake().started()) {
    if (mgr->handshake().remote_hdl == hdl) {
      BROKER_ERROR("multiple peering requests: already started peering with"
                   << remote_peer);
    } else {
      BROKER_ERROR("multiple peers share a single actor handle!");
    }
    return {};
  } else {
    auto& hs = mgr->handshake();
    if (hs.responder_start_peering(remote_peer, hdl)) {
      BROKER_DEBUG("start peering with" << remote_peer);
      BROKER_ASSERT(hs.out != caf::invalid_stream_slot);
      return {hs.out};
    } else {
      BROKER_ERROR("failed to start peering with" << remote_peer << ":"
                                                  << hs.err);
      return {};
    }
  }
}

caf::outbound_stream_slot<node_message, atom::ok, caf::actor, endpoint_id,
                          filter_type, lamport_timestamp>
stream_transport::handle_peering_handshake_1(caf::stream<node_message>,
                                             const caf::actor& hdl,
                                             const endpoint_id& remote_peer,
                                             const filter_type& filter,
                                             lamport_timestamp timestamp) {
  BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(remote_peer) << BROKER_ARG(filter)
                               << BROKER_ARG(timestamp));
  if (is_direct_connection(tbl(), remote_peer)) {
    BROKER_ERROR("drop peering handshake: already have a direct connection to"
                 << remote_peer);
    return {};
  } else if (auto mgr = get_pending(remote_peer); mgr == nullptr) {
    BROKER_ERROR("received open_stream_msg from an unknown responder");
    return {};
  } else if (mgr->handshake().remote_hdl != hdl) {
    BROKER_ERROR("multiple peers share a single actor handle!");
    return {};
  } else {
    if (mgr->handshake().originator_handle_open_stream_msg(filter, timestamp)) {
      return {mgr->handshake().out};
    } else {
      BROKER_ERROR("handshake failed:" << mgr->handshake().err);
      return {};
    }
  }
}

void stream_transport::handle_peering_handshake_2(
  caf::stream<node_message> in, atom::ok, const caf::actor& hdl,
  const endpoint_id& remote_peer, const filter_type& filter,
  lamport_timestamp timestamp) {
  BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(remote_peer) << BROKER_ARG(filter)
                               << BROKER_ARG(timestamp));
  if (auto mgr = get_pending(remote_peer); mgr == nullptr) {
    BROKER_ERROR("received open_stream_msg from an unknown originator");
  } else if (mgr->handshake().remote_hdl != hdl) {
    BROKER_ERROR("multiple peers share a single actor handle!");
  } else if (!mgr->handshake().responder_handle_open_stream_msg(filter,
                                                                timestamp)) {
    BROKER_ERROR("handshake failed:" << mgr->handshake().err);
  }
}

// -- callbacks ----------------------------------------------------------------

void stream_transport::shutdown(shutdown_options options) {
  BROKER_TRACE(BROKER_ARG(options));
  // TODO: honor wait-for-stores flag.
  auto drop_all = [](auto& container) {
    if (!container.empty()) {
      for (auto& sink : container) {
        sink->unobserve();
        sink->push();
      }
      container.clear();
    }
  };
  tearing_down_ = true;
  if (auto peers = peer_ids(); !peers.empty())
    for (auto& x : peers)
      unpeer(x);
  super::shutdown(options);
  drop_all(data_sinks_);
  drop_all(command_sinks_);
}

// -- "overridden" member functions of alm::peer -------------------------------

void stream_transport::handle_filter_update(endpoint_id_list& path,
                                            vector_timestamp& path_ts,
                                            const filter_type& filter) {
  BROKER_TRACE(BROKER_ARG(path) << BROKER_ARG(path_ts) << BROKER_ARG(filter));
  if (path.empty()) {
    BROKER_WARNING("drop message: path empty");
  } else if (auto mgr = get_pending(path.back()); mgr && mgr->blocks_inputs()) {
    auto msg = make_message(atom::subscribe_v, path, path_ts, filter);
    mgr->add_blocked_input(std::move(msg));
  } else {
    super::handle_filter_update(path, path_ts, filter);
  }
}

void stream_transport::handle_path_revocation(endpoint_id_list& path,
                                              vector_timestamp& path_ts,
                                              const endpoint_id& revoked_hop,
                                              const filter_type& filter) {
  BROKER_TRACE(BROKER_ARG(path)
               << BROKER_ARG(path_ts) << BROKER_ARG(revoked_hop)
               << BROKER_ARG(filter));
  if (path.empty()) {
    BROKER_WARNING("drop message: path empty");
  } else if (auto mgr = get_pending(path.back()); mgr && mgr->blocks_inputs()) {
    auto msg = make_message(atom::revoke_v, path, path_ts, revoked_hop, filter);
    mgr->add_blocked_input(std::move(msg));
  } else {
    super::handle_path_revocation(path, path_ts, revoked_hop, filter);
  }
}

// -- overrides for detail::central_dispatcher ---------------------------------

void stream_transport::flush() {
  for (auto& kvp: hdl_to_mgr_)
    kvp.second->push();
  for (auto& sink : data_sinks_)
    sink->push();
  for (auto& sink : command_sinks_)
    sink->push();
}

template <class T>
void stream_transport::dispatch_impl(const T& msg) {
  const auto& topic = get_topic(msg);
  if (auto i = dispatch_cache_.find(topic); i != dispatch_cache_.end()) {
    for (auto& [first_hop, path, receivers] : i->second)
      first_hop->enqueue(node_message{msg, path, receivers});
  } else {
    detail::prefix_matcher matches;
    endpoint_id_list receivers;
    for (const auto& [peer, filter] : peer_filters_)
      if (matches(filter, topic))
        receivers.emplace_back(peer);
    BROKER_DEBUG("got" << receivers.size() << "receiver for" << msg);
    dispatch_cache_entries entries;
    if (!receivers.empty()) {
      std::vector<alm::multipath> paths;
      std::vector<endpoint_id> unreachables;
      alm::multipath::generate(receivers, tbl_, paths, unreachables);
      for (auto&& path : paths) {
        // Move receivers into the path-specific list.
        endpoint_id_list msg_recs;
        msg_recs.reserve(receivers.size());
        for (auto i = receivers.begin(); i != receivers.end();) {
          if (path.contains(*i)) {
            msg_recs.emplace_back(std::move(*i));
            i = receivers.erase(i);
          } else {
            ++i;
          }
        }
        // Ship to the first hop.
        if (!msg_recs.empty()) {
          if (auto hop = peer_lookup(path.head())) {
            entries.emplace_back(dispatch_cache_entry{hop, path, msg_recs});
            auto nm = node_message{msg, std::move(path), std::move(msg_recs)};
            hop->enqueue(std::move(nm));
          } else {
            BROKER_WARNING("cannot ship message: no path to" << path.head());
          }
        } else {
          BROKER_ERROR("generate_paths produced a path without receivers!");
        }
      }
      if (!unreachables.empty())
        BROKER_WARNING("cannot ship message: no path to any of"
                       << unreachables);
    }
    if (!entries.empty())
      dispatch_cache_.emplace(topic, std::move(entries));
  }
}

void stream_transport::dispatch(const data_message& msg) {
  BROKER_TRACE(BROKER_ARG(msg));
  dispatch_impl(msg);
}

void stream_transport::dispatch(const command_message& msg) {
  BROKER_TRACE(BROKER_ARG(msg));
  dispatch_impl(msg);
}

void stream_transport::dispatch(node_message&& msg) {
  BROKER_TRACE(BROKER_ARG(msg));
  // Deconstruction and sanity checking. Can't use structured binding here due
  // to limitations on lambda captures.
  auto& tup = msg.unshared();
  auto& content = get<0>(tup);
  auto& path = get<1>(tup);
  auto& receivers = get<2>(tup);
  if (receivers.empty()) {
    BROKER_WARNING("received a node message with no receivers");
    return;
  }
  // Push to local subscribers if the message is addressed at this node and this
  // node is on the list of receivers.
  if (path.head() == id_) {
    auto predicate = [this](const endpoint_id& nid) { return nid == id_; };
    if (auto i = std::remove_if(receivers.begin(), receivers.end(), predicate);
        i != receivers.end()) {
      receivers.erase(i, receivers.end());
      publish_locally(content);
    }
    // Forward to all next hops.
    path.for_each_node([&](multipath&& next) {
      if (auto ptr = peer_lookup(next.head())) {
        ptr->enqueue(node_message{content, std::move(next), receivers});
      } else {
        BROKER_DEBUG("cannot ship message: no path to" << next.head());
      }
    });
  } else if (auto ptr = peer_lookup(path.head())) {
    ptr->enqueue(std::move(msg));
  } else {
    BROKER_DEBUG("cannot ship message: no path to" << path.head());
  }
}

// -- overrides for detail::unipath_manager::observer --------------------------

void stream_transport::closing(detail::unipath_manager* ptr, bool graceful,
                               const error& reason) {
  BROKER_ASSERT(ptr != nullptr);
  auto drop_from = [](auto& container, auto* dptr) {
    auto pred = [dptr](const auto& entry) { return entry == dptr; };
    auto i = std::find_if(container.begin(), container.end(), pred);
    if (i == container.end()) {
      return false;
    } else {
      container.erase(i, container.end());
      return true;
    }
  };
  auto f = detail::make_overload(
    [this, &reason](detail::peer_manager* dptr) {
      if (auto i = mgr_to_hdl_.find(dptr); i != mgr_to_hdl_.end()) {
        auto hdl = i->second;
        drop_peer(hdl, reason);
      }
    },
    [this, drop_from](detail::unipath_data_sink* dptr) {
      drop_from(data_sinks_, dptr);
    },
    [this, drop_from](detail::unipath_command_sink* dptr) {
      drop_from(command_sinks_, dptr);
    },
    [](detail::unipath_source*) {
      // nop
    });
  std::visit(f, ptr->derived_ptr());
}

void stream_transport::downstream_connected(detail::unipath_manager* ptr,
                                            const caf::actor&) {
  auto f = detail::make_overload(
    [](detail::peer_manager*) {
      // Nothing to do. We add state in finalize_handshake.
    },
    [this](detail::unipath_data_sink* derived_ptr) {
      data_sinks_.emplace_back(derived_ptr);
    },
    [this](detail::unipath_command_sink* derived_ptr) {
      command_sinks_.emplace_back(derived_ptr);
    },
    [](detail::unipath_source*) {
      BROKER_ERROR("downstream_connected called on a unipath_source");
    });
  std::visit(f, ptr->derived_ptr());
}

bool stream_transport::finalize_handshake(detail::peer_manager* mgr) {
  BROKER_TRACE("");
  auto add_mapping = [this, mgr] {
    auto hdl = mgr->handshake().remote_hdl;
    auto [i, added] = hdl_to_mgr_.emplace(hdl, mgr);
    if (!added)
      return false;
    if (mgr_to_hdl_.emplace(mgr, hdl).second) {
      return true;
    } else {
      hdl_to_mgr_.erase(i);
      return false;
    }
  };
  auto& hs = mgr->handshake();
  BROKER_ASSERT(hs.done());
  BROKER_ASSERT(hs.in != caf::invalid_stream_slot);
  BROKER_ASSERT(hs.out != caf::invalid_stream_slot);
  BROKER_ASSERT(hs.remote_hdl != nullptr);
  BROKER_ASSERT(mgr->hdl() == hs.remote_hdl);
  if (auto i = pending_.find(hs.remote_id); i != pending_.end()) {
    BROKER_ASSERT(i->second == mgr);
    pending_.erase(i);
    if (!add_mapping()) {
      BROKER_ERROR("failed add mapping for the peer manager");
      return false;
    } else if (is_direct_connection(tbl_, hs.remote_id)) {
      BROKER_ERROR("tried to complete handshake for already connected peer");
      mgr_to_hdl_.erase(mgr);
      hdl_to_mgr_.erase(hs.remote_hdl);
      return false;
    } else {
      auto trigger_peer_discovered = !reachable(tbl_, hs.remote_id);
      tbl_[hs.remote_id].hdl = hs.remote_hdl;
      if (trigger_peer_discovered)
        peer_discovered(hs.remote_id);
      peer_connected(hs.remote_id, hs.remote_hdl);
      auto path = std::vector<endpoint_id>{hs.remote_id};
      auto path_ts = vector_timestamp{hs.remote_timestamp};
      handle_filter_update(path, path_ts, hs.remote_filter);
      return true;
    }
  } else {
    BROKER_ERROR("finalize_handshake called but manager not found in pending_");
    return false;
  }
}

void stream_transport::abort_handshake(detail::peer_manager* mgr) {
  auto& hs = mgr->handshake();
  if (auto i = pending_.find(hs.remote_id); i != pending_.end()) {
    BROKER_ASSERT(!mgr->unique());
    pending_.erase(i);
  }
}

// -- initialization -----------------------------------------------------------

caf::behavior stream_transport::make_behavior() {
  using detail::lift;
  return caf::message_handler{
    // Expose to member functions to messaging API.
    lift<atom::peer, atom::init>(*this,
                                 &stream_transport::handle_peering_request),
    lift<>(*this, &stream_transport::handle_peering_handshake_1),
    lift<>(*this, &stream_transport::handle_peering_handshake_2),
    lift<atom::join>(*this, &stream_transport::add_worker),
    lift<atom::join>(*this, &stream_transport::add_sending_worker),
    lift<atom::join, atom::store>(*this, &stream_transport::add_sending_store),
    // Trigger peering to remotes.
    [this](atom::peer, const endpoint_id& remote_peer, const caf::actor& hdl) {
      start_peering(remote_peer, hdl, self()->make_response_promise());
    },
    // Per-stream subscription updates.
    [this](atom::join, atom::update, caf::stream_slot slot,
           filter_type& filter) {
      update_filter(slot, std::move(filter));
    },
    [this](atom::join, atom::update, caf::stream_slot slot, filter_type& filter,
           const caf::actor& listener) {
      auto res = update_filter(slot, std::move(filter));
      self()->send(listener, res);
    },
    // Allow local publishers to hook directly into the stream.
    [this](caf::stream<data_message> in) {
      make_unipath_source(this, in);
    },
    [this](caf::stream<node_message_content> in) {
      make_unipath_source(this, in);
    },
    // // Special handlers for bypassing streams and/or forwarding.
    [this](atom::publish, atom::local, data_message& msg) {
     publish_locally(msg);
    },
    [this](atom::unpeer, const caf::actor& hdl) { unpeer(hdl); },
    [this](atom::unpeer, const endpoint_id& peer_id) { unpeer(peer_id); },
  }
    .or_else(super::make_behavior());
}

// -- utility ------------------------------------------------------------------

bool stream_transport::update_filter(caf::stream_slot slot,
                                     filter_type&& filter) {
  auto predicate = [slot](const auto& mgr) {
    return mgr->outbound_path_slot() == slot;
  };
  auto fetch_from = [predicate](const auto& container) {
    auto i = std::find_if(container.begin(), container.end(), predicate);
    return std::make_pair(i, i != container.end());
  };
  if (auto [i, i_valid] = fetch_from(data_sinks_); i_valid) {
    auto& mgr = *i;
    subscribe(filter);
    mgr->filter(std::move(filter));
    return true;
  } else if (auto [j, j_valid] = fetch_from(command_sinks_); j_valid) {
    auto& mgr = *j;
    subscribe(filter);
    mgr->filter(std::move(filter));
    return true;
  } else {
    return false;
  }
}

bool stream_transport::peer_cleanup(const endpoint_id& peer_id,
                                    const error* reason) {
  bool result = false;
  // Check whether we disconnect from the peer during the handshake.
  if (auto mgr = get_pending(peer_id)) {
    result = true;
    mgr->handshake().fail(ec::peer_disconnect_during_handshake);
    BROKER_ASSERT(get_pending(peer_id) == nullptr);
  } else if (auto i = tbl_.find(peer_id); i != tbl_.end() && i->second.hdl) {
    result = true;
    auto hdl = i->second.hdl;
    if (auto j = hdl_to_mgr_.find(hdl); j != hdl_to_mgr_.end()) {
      j->second->unobserve();
      j->second->shutdown();
      mgr_to_hdl_.erase(j->second);
      hdl_to_mgr_.erase(j);
    } else {
      BROKER_DEBUG("found peer in routing table but not in hdl_to_mgr_");
    }
    if (reason)
      peer_disconnected(peer_id, hdl, *reason);
    else
      peer_removed(peer_id, hdl);
    erase_direct(tbl_, peer_id,
                 [this](const endpoint_id& whom) { peer_unreachable(whom); });
  }
  return result;
}

void stream_transport::drop_peer(const caf::actor& hdl, const error& reason) {
  endpoint_id remote_id;
  bool disconnected = false;
  if (auto peer_id = get_peer_id(tbl_, hdl)) {
    remote_id = *peer_id;
    peer_cleanup(*peer_id, &reason);
  } else {
    auto has_hdl = [&hdl](const auto& kvp) {
      return kvp.second->handshake().remote_hdl == hdl;
    };
    auto i = std::find_if(pending_.begin(), pending_.end(), has_hdl);
    if (i != pending_.end()) {
      auto remote_id = i->second->handshake().remote_id;
      peer_cleanup(remote_id, &reason);
    }
  }
}

void stream_transport::unpeer(const endpoint_id& peer_id,
                              const caf::actor& hdl) {
  BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG(hdl));
  if (!peer_cleanup(peer_id))
    cannot_remove_peer(peer_id);
}

void stream_transport::unpeer(const endpoint_id& peer_id) {
  BROKER_TRACE(BROKER_ARG(peer_id));
  if (auto i = tbl().find(peer_id); i != tbl().end()) {
    auto hdl = i->second.hdl;
    unpeer(peer_id, hdl);
  } else if (auto ptr = get_pending(peer_id)) {
    auto hdl = ptr->handshake().remote_hdl;
    unpeer(peer_id, hdl);
  } else {
    cannot_remove_peer(peer_id);
  }
}

void stream_transport::unpeer(const caf::actor& hdl) {
  BROKER_TRACE(BROKER_ARG(hdl));
  if (auto peer_id = get_peer_id(tbl(), hdl)) {
    unpeer(*peer_id, hdl);
  } else if (auto ptr = get_pending(hdl)) {
    auto peer_id = ptr->handshake().remote_id;
    unpeer(peer_id, hdl);
  } else {
    cannot_remove_peer(hdl);
  }
}

detail::peer_manager*
stream_transport::peer_lookup(const endpoint_id& peer_id) {
  if (auto i = pending_.find(peer_id); i != pending_.end())
    return i->second.get();
  if (auto row = find_row(tbl_, peer_id))
    if (auto i = hdl_to_mgr_.find(row->hdl); i != hdl_to_mgr_.end())
      return i->second.get();
  return nullptr;
}

} // namespace broker::alm
