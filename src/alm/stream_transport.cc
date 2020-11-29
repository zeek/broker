#include "broker/alm/stream_transport.hh"

namespace broker::alm {

// -- constructors, destructors, and assignment operators ----------------------

stream_transport::stream_transport(caf::event_based_actor* self)
  : super(self), caf::stream_manager(self), out_(this) {
  continuous(true);
}

// -- properties ---------------------------------------------------------------

optional<caf::stream_slot>
stream_transport::output_slot(const caf::actor& hdl) const noexcept {
  auto i = hdl_to_ostream_.find(hdl);
  if (i == hdl_to_ostream_.end())
    return nil;
  return i->second;
}

optional<caf::stream_slot>
stream_transport::input_slot(const caf::actor& hdl) const noexcept {
  auto i = hdl_to_istream_.find(hdl);
  if (i == hdl_to_istream_.end())
    return nil;
  return i->second;
}

bool stream_transport::connected_to(const caf::actor& hdl) const noexcept {
  return output_slot(hdl) && input_slot(hdl);
}

stream_transport::handshake_ptr stream_transport::pending_connection(
  const endpoint_id& peer_id) const noexcept {
  if (auto i = pending_connections_.find(peer_id);
      i != pending_connections_.end()) {
    BROKER_ASSERT(i->second != nullptr);
    return i->second;
  } else {
    return nullptr;
  }
}

stream_transport::handshake_ptr
stream_transport::pending_connection_or_insert(const endpoint_id& peer_id) {
  if (auto i = pending_connections_.find(peer_id);
      i != pending_connections_.end()) {
    BROKER_ASSERT(i->second != nullptr);
    return i->second;
  } else {
    auto result = caf::make_counted<handshake_type>(this);
    pending_connections_.emplace(peer_id, result);
    return result;
  }
}

stream_transport::handshake_ptr
stream_transport::pending_connection(const caf::actor& hdl) const noexcept {
  for (auto& kvp : pending_connections_)
    if (kvp.second->remote_hdl == hdl)
      return kvp.second;
  return nullptr;
}

stream_transport::handshake_ptr
stream_transport::pending_slot(caf::stream_slot in) const noexcept {
  if (auto i = pending_slots_.find(in); i != pending_slots_.end()) {
    BROKER_ASSERT(i->second != nullptr);
    return i->second;
  } else {
    return nullptr;
  }
}

stream_transport::peer_trait::manager&
stream_transport::peer_manager() noexcept {
  return out_.get<peer_trait::manager>();
}

stream_transport::worker_trait::manager&
stream_transport::worker_manager() noexcept {
  return out_.get<worker_trait::manager>();
}

stream_transport::store_trait::manager&
stream_transport::store_manager() noexcept {
  return out_.get<store_trait::manager>();
}

// -- adding local subscribers -------------------------------------------------

stream_transport::worker_slot
stream_transport::add_sending_worker(const filter_type& filter) {
  BROKER_TRACE(BROKER_ARG(filter));
  using element_type = typename worker_trait::element;
  auto slot = add_unchecked_outbound_path<element_type>();
  if (slot != caf::invalid_stream_slot) {
    subscribe(filter);
    out_.assign<worker_trait::manager>(slot);
    worker_manager().set_filter(slot, filter);
  }
  return slot;
}

error stream_transport::add_worker(const caf::actor& hdl,
                                   const filter_type& filter) {
  BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(filter));
  using element_type = typename worker_trait::element;
  auto slot = add_unchecked_outbound_path<element_type>(hdl);
  if (slot == caf::invalid_stream_slot)
    return caf::sec::cannot_add_downstream;
  subscribe(filter);
  out_.template assign<typename worker_trait::manager>(slot);
  worker_manager().set_filter(slot, filter);
  return caf::none;
}

stream_transport::store_slot
stream_transport::add_sending_store(const filter_type& filter) {
  BROKER_TRACE(BROKER_ARG(filter));
  using element_type = typename store_trait::element;
  auto slot = add_unchecked_outbound_path<element_type>();
  if (slot != caf::invalid_stream_slot) {
    subscribe(filter);
    out_.template assign<typename store_trait::manager>(slot);
    store_manager().set_filter(slot, filter);
  }
  return slot;
}

error stream_transport::add_store(const caf::actor& hdl,
                                  const filter_type& filter) {
  BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(filter));
  using element_type = typename store_trait::element;
  auto slot = add_unchecked_outbound_path<element_type>(hdl);
  if (slot == caf::invalid_stream_slot)
    return caf::sec::cannot_add_downstream;
  subscribe(filter);
  out_.template assign<typename store_trait::manager>(slot);
  store_manager().set_filter(slot, filter);
  return caf::none;
}

// -- sending ------------------------------------------------------------------

void stream_transport::send(const caf::actor& receiver, atom::publish,
                            node_message content) {
  BROKER_TRACE(BROKER_ARG(receiver) << BROKER_ARG(content));
  // Fetch the output slot for reaching the receiver.
  if (auto i = hdl_to_ostream_.find(receiver); i != hdl_to_ostream_.end()) {
    auto slot = i->second;
    BROKER_DEBUG("push to slot" << slot);
    // Fetch the buffer for that slot and enqueue the message.
    auto& nested = out_.template get<typename peer_trait::manager>();
    if (!nested.push_to(slot, std::move(content)))
      BROKER_WARNING("unable to access state for output slot");
  } else {
    BROKER_WARNING("unable to locate output slot for receiver");
  }
}

void stream_transport::send(const caf::actor& receiver, atom::subscribe,
                            const endpoint_id_list& path,
                            const vector_timestamp& ts,
                            const filter_type& new_filter) {
  BROKER_TRACE(BROKER_ARG(receiver)
               << BROKER_ARG(path) << BROKER_ARG(ts) << BROKER_ARG(new_filter));
  self()->send(receiver, atom::subscribe_v, path, ts, new_filter);
}

void stream_transport::send(const caf::actor& receiver, atom::revoke,
                            const endpoint_id_list& path,
                            const vector_timestamp& ts,
                            const endpoint_id& lost_peer,
                            const filter_type& new_filter) {
  BROKER_TRACE(BROKER_ARG(receiver)
               << BROKER_ARG(path) << BROKER_ARG(ts) << BROKER_ARG(lost_peer)
               << BROKER_ARG(new_filter));
  self()->send(receiver, atom::revoke_v, path, ts, lost_peer, new_filter);
}

// -- peering ------------------------------------------------------------------

// Initiates peering between A (this node) and B (remote peer).
void stream_transport::start_peering(const endpoint_id& remote_peer,
                                     const caf::actor& hdl,
                                     caf::response_promise rp) {
  BROKER_TRACE(BROKER_ARG(remote_peer) << BROKER_ARG(hdl));
  if (direct_connection(tbl(), remote_peer)) {
    BROKER_DEBUG("start_peering ignored: already peering with" << remote_peer);
    rp.deliver(atom::peer_v, atom::ok_v, hdl);
  } else if (remote_peer < id()) {
    // We avoid conflicts in the handshake process by always having the node
    // with the smaller ID initiate the peering. Otherwise, we could end up in
    // a deadlock during handshake if both sides send step 1 at the same time.
    auto hs = pending_connection_or_insert(remote_peer);
    if (!hs->started()) {
      hs->to_responder();
    } else if (!hs->is_responder()) {
      BROKER_ERROR("peer tries to obtained wrong role in handshake!");
      rp.deliver(make_error(ec::invalid_handshake_state));
      return;
    }
    if (rp.pending())
      hs->promises.emplace_back(rp);
    auto s = self();
    auto strong_ptr = caf::intrusive_ptr<stream_transport>{this};
    s->request(hdl, std::chrono::minutes(10), atom::peer_v, id(), s)
      .then(
        [](atom::peer, atom::ok, const endpoint_id&) {
          // nop
        },
        [strong_this{std::move(strong_ptr)}, hs](caf::error& err) mutable {
          // Abort the handshake if it hasn't started yet. Otherwise, we
          // have other mechanisms in place that capture the same error.
          if (!hs->started()) {
            BROKER_DEBUG("peering failed:" << err);
            hs->fail(std::move(err));
            strong_this->cleanup(std::move(hs));
          }
        });
  } else if (auto hs = pending_connection(remote_peer); hs != nullptr) {
    if (hs->remote_hdl == hdl) {
      BROKER_DEBUG("start_peering ignored: already started peering with"
                   << remote_peer);
      rp.deliver(atom::peer_v, atom::ok_v, hdl);
    } else {
      BROKER_ERROR("multiple peers share a single actor handle!");
      rp.deliver(make_error(ec::invalid_peering_request,
                            "handle already in use by another responder"));
    }
  } else {
    auto new_hs = caf::make_counted<handshake_type>(this);
    if (new_hs->originator_start_peering(remote_peer, hdl, std::move(rp))) {
      BROKER_DEBUG("start peering with" << remote_peer);
      pending_connections_.emplace(remote_peer, std::move(new_hs));
    } else {
      BROKER_ERROR("failed to start peering with" << remote_peer << ":"
                                                  << new_hs->err);
    }
  }
}

caf::outbound_stream_slot<node_message, caf::actor, endpoint_id, filter_type,
                          lamport_timestamp>
stream_transport::handle_peering_request(const endpoint_id& remote_peer,
                                         const caf::actor& hdl) {
  BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(remote_peer));
  if (direct_connection(tbl(), remote_peer)) {
    BROKER_ERROR("drop peering request: already have a direct connection to"
                 << remote_peer);
    return {};
  } else if (auto hs = pending_connection_or_insert(remote_peer);
             hs->started()) {
    if (hs->remote_hdl == hdl) {
      BROKER_ERROR("multiple peering requests: already started peering with"
                   << remote_peer);
    } else {
      BROKER_ERROR("multiple peers share a single actor handle!");
    }
    return {};
  } else {
    if (hs->responder_start_peering(remote_peer, hdl)) {
      BROKER_DEBUG("start peering with" << remote_peer);
      BROKER_ASSERT(hs->out != caf::invalid_stream_slot);
      return {hs->out};
    } else {
      BROKER_ERROR("failed to start peering with" << remote_peer << ":"
                                                  << hs->err);
      cleanup(std::move(hs));
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
  if (auto hs = pending_connection(remote_peer); hs == nullptr) {
    BROKER_ERROR("received open_stream_msg from an unknown responder");
    return {};
  } else if (hs->remote_hdl != hdl) {
    BROKER_ERROR("multiple peers share a single actor handle!");
    hs->fail(ec::invalid_peering_request);
    cleanup(std::move(hs));
    return {};
  } else {
    if (hs->originator_handle_open_stream_msg(filter, timestamp)) {
      auto result = hs->out;
      cleanup_and_replay_buffer_if_done(std::move(hs));
      return {result};
    } else {
      BROKER_ERROR("handshake failed:" << hs->err);
      cleanup(std::move(hs));
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
  if (auto hs = pending_connection(remote_peer); hs == nullptr) {
    BROKER_ERROR("received open_stream_msg from an unknown originator");
  } else if (hs->remote_hdl != hdl) {
    BROKER_ERROR("multiple peers share a single actor handle!");
    hs->fail(ec::invalid_peering_request);
    cleanup(std::move(hs));
  } else {
    if (hs->responder_handle_open_stream_msg(filter, timestamp)) {
      cleanup_and_replay_buffer_if_done(std::move(hs));
    } else {
      BROKER_ERROR("handshake failed:" << hs->err);
      cleanup(std::move(hs));
    }
  }
}

void stream_transport::cleanup(handshake_ptr ptr) {
  pending_connections_.erase(ptr->remote_id);
  pending_slots_.erase(ptr->in);
  pending_slots_.erase(ptr->out);
}

void stream_transport::cleanup_and_replay_buffer_if_done(handshake_ptr ptr) {
  if (ptr->done()) {
    BROKER_DEBUG("completed handshake to" << ptr->remote_id);
    cleanup(ptr);
    ptr->replay_input_buffer();
  }
}

caf::stream_slot stream_transport::make_inbound_path(handshake_type* hs) {
  BROKER_TRACE(BROKER_ARG2("peer", hs->remote_id));
  auto res = add_unchecked_inbound_path(caf::stream<node_message>{});
  BROKER_DEBUG("opened input slot" << res << "for peer" << hs->remote_id);
  pending_slots_.emplace(res, hs);
  return res;
}

caf::stream_slot stream_transport::make_outbound_path(handshake_type* hs) {
  BROKER_TRACE(BROKER_ARG2("peer", hs->remote_id));
  auto result = caf::invalid_stream_slot;
  auto self_hdl = caf::actor_cast<caf::actor>(self());
  if (hs->is_originator()) {
    auto data = std::make_tuple(atom::ok_v, std::move(self_hdl), id(), filter(),
                                timestamp());
    result = add_unchecked_outbound_path<node_message>(hs->remote_hdl,
                                                       std::move(data));
  } else {
    BROKER_ASSERT(hs->is_responder());
    auto data
      = std::make_tuple(std::move(self_hdl), id(), filter(), timestamp());
    result = add_unchecked_outbound_path<node_message>(hs->remote_hdl,
                                                       std::move(data));
  }
  BROKER_ASSERT(result != caf::invalid_stream_slot);
  BROKER_DEBUG("opened output slot" << result << "for peer" << hs->remote_id);
  out_.template assign<typename peer_trait::manager>(result);
  pending_slots_.emplace(result, hs);
  return result;
}

bool stream_transport::finalize(handshake_type* hs) {
  BROKER_TRACE(BROKER_ARG2("peer", hs->remote_id));
  BROKER_ASSERT(hs->in != caf::invalid_stream_slot);
  BROKER_ASSERT(hs->out != caf::invalid_stream_slot);
  BROKER_ASSERT(hs->remote_hdl != nullptr);
  if (direct_connection(tbl(), hs->remote_id)) {
    BROKER_ERROR("tried to complete handshake for already connected peer");
    return false;
  } else if (!hdl_to_istream_.emplace(hs->remote_hdl, hs->in).second) {
    BROKER_ERROR("peer handle already exists in hdl_to_istream");
    return false;
  } else if (!hdl_to_ostream_.emplace(hs->remote_hdl, hs->out).second) {
    BROKER_ERROR("peer handle already exists in hdl_to_ostream");
    return false;
  } else {
    auto& table = tbl();
    auto trigger_peer_discovered = !reachable(table, hs->remote_id);
    table[hs->remote_id].hdl = hs->remote_hdl;
    if (trigger_peer_discovered)
      peer_discovered(hs->remote_id);
    peer_connected(hs->remote_id, hs->remote_hdl);
    auto path = std::vector<endpoint_id>{hs->remote_id};
    auto path_ts = vector_timestamp{hs->remote_timestamp};
    handle_filter_update(path, path_ts, hs->remote_filter);
    return true;
  }
}

void stream_transport::handle_buffered_msg(caf::inbound_path*,
                                           caf::outbound_path*,
                                           node_message& msg) {
  handle_publication(msg);
}

void stream_transport::handle_buffered_msg(caf::inbound_path*,
                                           caf::outbound_path*,
                                           caf::message& msg) {
  // TODO: we can make this more efficient with CAF 0.18 typed views.
  if (msg.match_elements<endpoint_id_list, vector_timestamp, filter_type>()) {
    handle_filter_update(msg.get_mutable_as<endpoint_id_list>(0),
                         msg.get_mutable_as<vector_timestamp>(1),
                         msg.get_as<filter_type>(2));
  } else if (msg.match_elements<endpoint_id_list, vector_timestamp, endpoint_id,
                                filter_type>()) {
    handle_path_revocation(msg.get_mutable_as<endpoint_id_list>(0),
                           msg.get_mutable_as<vector_timestamp>(1),
                           msg.get_as<endpoint_id>(2),
                           msg.get_as<filter_type>(3));
  } else {
    BROKER_ERROR("unexpected buffered message:" << msg);
  }
}

// -- callbacks ----------------------------------------------------------------

void stream_transport::ship_locally(const data_message& msg) {
  if (!worker_manager().paths().empty())
    worker_manager().push(msg);
}

void stream_transport::ship_locally(const command_message& msg) {
  if (!store_manager().paths().empty())
    store_manager().push(msg);
}

void stream_transport::shutdown(shutdown_options options) {
  BROKER_TRACE(BROKER_ARG(options));
  if (!store_manager().paths().empty()) {
    // TODO: honor wait-for-stores flag.
    auto cancel = make_error(ec::shutting_down);
    auto xs = store_manager().open_path_slots();
    for (auto x : xs)
      out_.remove_path(x, cancel, false);
  }
  if (auto peers = peer_ids(); !peers.empty()) {
    for (auto& x : peers)
      unpeer(x);
  }
  tearing_down_ = true;
  continuous(false);
  super::shutdown(options);
}

// -- "overridden" member functions of alm::peer -------------------------------

void stream_transport::handle_filter_update(endpoint_id_list& path,
                                            vector_timestamp& path_ts,
                                            const filter_type& filter) {
  BROKER_TRACE(BROKER_ARG(path) << BROKER_ARG(path_ts) << BROKER_ARG(filter));
  if (path.empty()) {
    BROKER_WARNING("drop message: path empty");
  } else if (auto hs = pending_connection(path.back()); hs != nullptr) {
    auto msg = caf::make_message(std::move(path), std::move(path_ts), filter);
    hs->input_buffer_emplace(std::move(msg));
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
  } else if (auto hs = pending_connection(path.back()); hs != nullptr) {
    auto msg = caf::make_message(std::move(path), std::move(path_ts),
                                 revoked_hop, filter);
    hs->input_buffer_emplace(std::move(msg));
  } else {
    super::handle_path_revocation(path, path_ts, revoked_hop, filter);
  }
}

// -- overridden member functions of caf::stream_manager ---------------------

#if CAF_VERSION >= 1800
bool stream_transport::congested(const caf::inbound_path& path) const noexcept {
  // The default assumes that all inbound paths write to the same output. This
  // is not true for Broker. We have two paths per peer: one inbound and one
  // outbound. We can safely assume that any batch we receive on the inbound
  // path won't send data to the outbound path of the same peer.
  size_t num_paths = 0;
  size_t max_buf_size = 0;
  size_t max_batch_size = 0;
  out_.for_each_path([&](const caf::outbound_path& x) {
    // Ignore the output path to the peer.
    if (x.hdl == path.hdl)
      return;
    ++num_paths;
    max_buf_size = std::max(max_buf_size, out_.buffered(x.slots.sender));
    max_batch_size
      = std::max(max_batch_size, static_cast<size_t>(x.desired_batch_size));
  });
  if (num_paths == 0) {
    // We can't be congested without downstream pahts.
    return false;
  }
  // Store up to two full batches.
  return max_buf_size >= (max_batch_size * 2);
}
#endif

void stream_transport::handle(caf::inbound_path* path,
                              caf::downstream_msg::batch& batch)  {
  BROKER_TRACE(BROKER_ARG(path) << BROKER_ARG(batch));
  if (auto hs = pending_slot(path->slots.receiver); hs == nullptr) {
    using peer_batch = typename peer_trait::batch;
    if (batch.xs.template match_elements<peer_batch>()) {
      for (auto& x : batch.xs.template get_mutable_as<peer_batch>(0))
        handle_publication(x);
      return;
    }
    auto try_publish = [&, this](auto trait) {
      using batch_type = typename decltype(trait)::batch;
      if (batch.xs.template match_elements<batch_type>()) {
        for (auto& x : batch.xs.template get_mutable_as<batch_type>(0))
          publish(x);
        return true;
      }
      return false;
    };
    if (try_publish(worker_trait{}) || try_publish(store_trait{})
        || try_publish(var_trait{}))
      return;
    BROKER_ERROR("unexpected batch:" << deep_to_string(batch));
  } else {
    hs->input_buffer_emplace(std::move(batch));
  }
}

void stream_transport::handle(caf::inbound_path* path,
                              caf::downstream_msg::close& x) {
  BROKER_TRACE(BROKER_ARG(path) << BROKER_ARG(x));
  if (auto hs = pending_slot(path->slots.receiver); hs == nullptr)
    handle_impl(path, x);
  else
    hs->input_buffer_emplace(std::move(x));
}

void stream_transport::handle(caf::inbound_path* path,
                              caf::downstream_msg::forced_close& x) {
  BROKER_TRACE(BROKER_ARG(path) << BROKER_ARG(x));
  if (auto hs = pending_slot(path->slots.receiver); hs == nullptr)
    handle_impl(path, x);
  else
    hs->input_buffer_emplace(std::move(x));
}

void stream_transport::handle(caf::stream_slots slots,
                              caf::upstream_msg::ack_batch& x) {
  BROKER_TRACE(BROKER_ARG(slots) << BROKER_ARG(x));
  if (auto hs = pending_slot(slots.receiver); hs == nullptr)
    caf::stream_manager::handle(slots, x);
  else
    hs->input_buffer_emplace(std::move(x));
}

void stream_transport::handle(caf::stream_slots slots,
                              caf::upstream_msg::drop& x) {
  BROKER_TRACE(BROKER_ARG(slots) << BROKER_ARG(x));
  if (auto hs = pending_slot(slots.receiver); hs == nullptr)
    handle_impl(slots, x);
  else
    hs->input_buffer_emplace(std::move(x));
}

void stream_transport::handle(caf::stream_slots slots,
                              caf::upstream_msg::forced_drop& x) {
  BROKER_TRACE(BROKER_ARG(slots) << BROKER_ARG(x));
  if (auto hs = pending_slot(slots.receiver); hs == nullptr)
    handle_impl(slots, x);
  else
    hs->input_buffer_emplace(std::move(x));
}

bool stream_transport::handle(caf::stream_slots slots,
                              caf::upstream_msg::ack_open& x) {
  BROKER_TRACE(BROKER_ARG(slots) << BROKER_ARG(x));
  if (peer_manager().path(slots.receiver) == nullptr) {
    // We only add custom logic for peer messages.
    return !tearing_down_ && caf::stream_manager::handle(slots, x);
  } else if (auto hs = pending_slot(slots.receiver); hs == nullptr) {
    BROKER_ERROR("received ack_open on a path not marked as pending");
    return false;
  } else if (x.rebind_from != x.rebind_to) {
    BROKER_ERROR("peers are not allowed to transfer streams!");
    hs->fail(ec::invalid_handshake_state);
    cleanup(std::move(hs));
    return false;
  } else if (tearing_down_) {
    hs->fail(ec::shutting_down);
    cleanup(std::move(hs));
    return false;
  } else {
    if (caf::stream_manager::handle(slots, x)) {
      if (hs->handle_ack_open_msg()) {
        cleanup_and_replay_buffer_if_done(std::move(hs));
        return true;
      } else {
        BROKER_ERROR("handshake failed:" << hs->err);
        cleanup(std::move(hs));
        return false;
      }
    } else {
      BROKER_ERROR("handshake failed: stream_manager::handle returned false");
      hs->fail(ec::invalid_handshake_state);
      cleanup(std::move(hs));
      return false;
    }
  }
}

bool stream_transport::done() const {
  return !continuous() && pending_handshakes_ == 0 && inbound_paths_.empty()
         && out_.clean();
}

bool stream_transport::idle() const noexcept {
  // Same as `stream_stage<...>`::idle().
  return out_.stalled() || (out_.clean() && this->inbound_paths_idle());
}

stream_transport::downstream_manager_type& stream_transport::out() {
  return out_;
}

void stream_transport::unpeer(const endpoint_id& peer_id,
                              const caf::actor& hdl) {
  BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG(hdl));
  // Keeps track of how many successful cleanup steps we perform. If this
  // counter is still 0 at the end of this function than this function was
  // called with an unknown peer.
  size_t cleanup_steps = 0;
  // Check whether we disconnect from the peer during the handshake.
  if (auto hs = pending_connection(peer_id); hs != nullptr) {
    ++cleanup_steps;
    hs->fail(ec::peer_disconnect_during_handshake);
    cleanup(std::move(hs));
  } else {
    // Close the associated outbound path.
    if (auto i = hdl_to_ostream_.find(hdl); i != hdl_to_ostream_.end()) {
      ++cleanup_steps;
      out_.close(i->second);
      hdl_to_ostream_.erase(i);
    }
    // Close the associated inbound path.
    if (auto i = hdl_to_istream_.find(hdl); i != hdl_to_istream_.end()) {
      ++cleanup_steps;
      error reason;
      remove_input_path(i->second, reason, false);
      hdl_to_istream_.erase(i);
    }
  }
  // The callback peer_removed ultimately removes the entry from the routing
  // table. Since some implementations of peer_removed may still query the
  // routing table, we only check whether peer_id exists in the routing table
  // rather than calling `erase`.
  cleanup_steps += tbl().count(peer_id);
  if (cleanup_steps > 0)
    peer_removed(peer_id, hdl);
  else
    cannot_remove_peer(peer_id);
}

void stream_transport::unpeer(const endpoint_id& peer_id) {
  BROKER_TRACE(BROKER_ARG(peer_id));
  if (auto i = tbl().find(peer_id); i != tbl().end()) {
    auto hdl = i->second.hdl;
    unpeer(peer_id, hdl);
  } else if (auto hs = pending_connection(peer_id); hs != nullptr) {
    auto hdl = hs->remote_hdl;
    unpeer(peer_id, hdl);
  } else {
    cannot_remove_peer(peer_id);
  }
}

void stream_transport::unpeer(const caf::actor& hdl) {
  BROKER_TRACE(BROKER_ARG(hdl));
  if (auto peer_id = get_peer_id(tbl(), hdl)) {
    unpeer(*peer_id, hdl);
  } else if (auto hs = pending_connection(hdl); hs != nullptr) {
    auto peer_id = hs->remote_id;
    unpeer(peer_id, hdl);
  } else {
    cannot_remove_peer(hdl);
  }
}

} // namespace broker::alm
