#pragma once

#include <unordered_map>
#include <vector>

#include <caf/broadcast_downstream_manager.hpp>
#include <caf/cow_tuple.hpp>
#include <caf/detail/scope_guard.hpp>
#include <caf/detail/unordered_flat_map.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/fused_downstream_manager.hpp>

#include "broker/alm/peer.hh"
#include "broker/alm/routing_table.hh"
#include "broker/detail/lift.hh"
#include "broker/detail/peer_handshake.hh"
#include "broker/detail/prefix_matcher.hh"
#include "broker/error.hh"
#include "broker/filter_type.hh"
#include "broker/logger.hh"
#include "broker/message.hh"
#include "broker/shutdown_options.hh"

namespace broker::alm {

/// The transport registers these message handlers:
///
/// ~~~
/// (atom::peer, peer_id_type, actor) -> void
/// => start_peering(id, hdl)
///
/// (atom::peer, atom::init, peer_id_type, actor) -> slot
/// => handle_peering_request(...)
///
/// (stream<node_message>, actor, peer_id_type, filter_type, lamport_timestamp) -> slot
/// => handle_peering_handshake_1(...)
///
/// (stream<node_message>, actor, peer_id_type) -> void
/// => handle_peering_handshake_2(...)
///
/// (atom::unpeer, actor hdl) -> void
/// => disconnect(hdl)
/// ~~~
template <class Derived, class PeerId>
class stream_transport : public peer<Derived, PeerId, caf::actor>,
                         public caf::stream_manager {
public:
  // -- member types -----------------------------------------------------------

  using super = peer<Derived, PeerId, caf::actor>;

  using peer_id_type = PeerId;

  using peer_id_list = std::vector<peer_id_type>;

  using message_type = typename super::message_type;

  using handshake_type = detail::peer_handshake<stream_transport>;

  using handshake_ptr = detail::peer_handshake_ptr<stream_transport>;

  /// Helper trait for defining streaming-related types for local actors
  /// (workers and stores).
  template <class T>
  struct local_trait {
    /// Type of a single element in the stream.
    using element = caf::cow_tuple<topic, T>;

    /// Type of a full batch in the stream.
    using batch = std::vector<element>;

    /// Type of the downstream_manager that broadcasts data to local actors.
    using manager = caf::broadcast_downstream_manager<element, filter_type,
                                                      detail::prefix_matcher>;
  };

  /// Streaming-related types for workers.
  using worker_trait = local_trait<data>;

  /// Streaming-related types for stores.
  using store_trait = local_trait<internal_command>;

  /// Streaming-related types for sources that produce both types of messages.
  struct var_trait {
    using batch = std::vector<node_message_content>;
  };

  /// Streaming-related types for peers.
  struct peer_trait {
    /// Type of a single element in the stream.
    using element = message_type;

    using batch = std::vector<element>;

    /// Type of the downstream_manager that broadcasts data to local actors.
    using manager = caf::broadcast_downstream_manager<element>;
  };

  /// Composed downstream_manager type for bundled dispatching.
  using downstream_manager_type
    = caf::fused_downstream_manager<typename peer_trait::manager,
                                    typename worker_trait::manager,
                                    typename store_trait::manager>;

  /// Maps a peer handle to the slot for outbound communication. Our routing
  /// table translates peer IDs to actor handles, but we need one additional
  /// step to get to the associated stream.
  using hdl_to_slot_map
    = caf::detail::unordered_flat_map<caf::actor, caf::stream_slot>;

  // -- imported member functions (silence hiding warnings) --------------------

  using caf::stream_manager::shutdown;

  // -- constructors, destructors, and assignment operators --------------------

  explicit stream_transport(caf::event_based_actor* self)
    : super(self), caf::stream_manager(self), out_(this) {
    continuous(true);
  }

  // -- properties -------------------------------------------------------------

  auto tbl() noexcept -> decltype(super::tbl()) {
    return super::tbl();
  }

  auto&& local_id() {
    return dref().id();
  }

  caf::event_based_actor* self() noexcept {
    // Our only constructor accepts an event-based actor. Hence, we know for
    // sure that this case is safe, even though the base type stores self_ as a
    // scheduled_actor pointer.
    return static_cast<caf::event_based_actor*>(this->self_);
  }

  /// Returns the slot for outgoing traffic to `hdl`.
  optional<caf::stream_slot> output_slot(const caf::actor& hdl) const noexcept {
    auto i = hdl_to_ostream_.find(hdl);
    if (i == hdl_to_ostream_.end())
      return nil;
    return i->second;
  }

  /// Returns the slot for incoming traffic from `hdl`.
  optional<caf::stream_slot> input_slot(const caf::actor& hdl) const noexcept {
    auto i = hdl_to_istream_.find(hdl);
    if (i == hdl_to_istream_.end())
      return nil;
    return i->second;
  }

  /// Returns whether this manager has inbound and outbound streams from and to
  /// `hdl`.`
  bool connected_to(const caf::actor& hdl) const noexcept {
    return output_slot(hdl) && input_slot(hdl);
  }

  const auto& pending_connections() const noexcept {
    return pending_connections_;
  }

  /// Returns the handshake state for `peer_id` or `nullptr`.
  handshake_ptr pending_connection(const peer_id_type& peer_id) const noexcept {
    if (auto i = pending_connections_.find(peer_id);
        i != pending_connections_.end()) {
      BROKER_ASSERT(i->second != nullptr);
      return i->second;
    } else {
      return nullptr;
    }
  }

  /// Like `pending_connection`, but inserts and returns a new handshake object
  /// if none exists yet.
  handshake_ptr pending_connection_or_insert(const peer_id_type& peer_id) {
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

  /// Returns the handshake state for `peer_id` or `nullptr`.
  /// @note This overload has complexity `O(n)`.
  handshake_ptr pending_connection(const caf::actor& hdl) const noexcept {
    for (auto& kvp : pending_connections_)
      if (kvp.second->remote_hdl == hdl)
        return kvp.second;
    return nullptr;
  }

  /// Returns the handshake state for `in` or `nullptr`.
  handshake_ptr pending_slot(caf::stream_slot in) const noexcept {
    if (auto i = pending_slots_.find(in); i != pending_slots_.end()) {
      BROKER_ASSERT(i->second != nullptr);
      return i->second;
    } else {
      return nullptr;
    }
  }

  auto& peer_manager() noexcept {
    return out_.template get<typename peer_trait::manager>();
  }

  auto& worker_manager() noexcept {
    return out_.template get<typename worker_trait::manager>();
  }

  auto& store_manager() noexcept {
    return out_.template get<typename store_trait::manager>();
  }

  // -- adding local subscribers -----------------------------------------------

  /// Subscribes `self->current_sender()` to `worker_manager()`.
  auto add_sending_worker(const filter_type& filter) {
    BROKER_TRACE(BROKER_ARG(filter));
    using element_type = typename worker_trait::element;
    using result_type = caf::outbound_stream_slot<element_type>;
    auto slot = add_unchecked_outbound_path<element_type>();
    if (slot != caf::invalid_stream_slot) {
      dref().subscribe(filter);
      out_.template assign<typename worker_trait::manager>(slot);
      worker_manager().set_filter(slot, filter);
    }
    return result_type{slot};
  }

  /// Subscribes `hdl` to `worker_manager()`.
  caf::error add_worker(const caf::actor& hdl, const filter_type& filter) {
    BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(filter));
    using element_type = typename worker_trait::element;
    auto slot = add_unchecked_outbound_path<element_type>(hdl);
    if (slot == caf::invalid_stream_slot)
      return caf::sec::cannot_add_downstream;
    dref().subscribe(filter);
    out_.template assign<typename worker_trait::manager>(slot);
    worker_manager().set_filter(slot, filter);
    return caf::none;
  }

  /// Subscribes `self->sender()` to `store_manager()`.
  auto add_sending_store(const filter_type& filter) {
    BROKER_TRACE(BROKER_ARG(filter));
    using element_type = typename store_trait::element;
    using result_type = caf::outbound_stream_slot<element_type>;
    auto slot = add_unchecked_outbound_path<element_type>();
    if (slot != caf::invalid_stream_slot) {
      dref().subscribe(filter);
      out_.template assign<typename store_trait::manager>(slot);
      store_manager().set_filter(slot, filter);
    }
    return result_type{slot};
  }

  /// Subscribes `hdl` to `store_manager()`.
  caf::error add_store(const caf::actor& hdl, const filter_type& filter) {
    BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(filter));
    using element_type = typename store_trait::element;
    auto slot = add_unchecked_outbound_path<element_type>(hdl);
    if (slot == caf::invalid_stream_slot)
      return caf::sec::cannot_add_downstream;
    dref().subscribe(filter);
    out_.template assign<typename store_trait::manager>(slot);
    store_manager().set_filter(slot, filter);
    return caf::none;
  }

  // -- publish and subscribe functions ----------------------------------------

  void publish_locally(data_message& msg) {
    worker_manager().push(msg);
  }

  void publish_locally(command_message& msg) {
    store_manager().push(msg);
  }

  // -- sending ----------------------------------------------------------------

  void stream_send(const caf::actor& receiver, message_type& msg) {
    BROKER_TRACE(BROKER_ARG(receiver) << BROKER_ARG(msg));
    // Fetch the output slot for reaching the receiver.
    if (auto i = hdl_to_ostream_.find(receiver); i != hdl_to_ostream_.end()) {
      auto slot = i->second;
      BROKER_DEBUG("push to slot" << slot);
      // Fetch the buffer for that slot and enqueue the message.
      auto& nested = out_.template get<typename peer_trait::manager>();
      if (!nested.push_to(slot, std::move(msg)))
        BROKER_WARNING("unable to access state for output slot");
    } else {
      BROKER_WARNING("unable to locate output slot for receiver");
    }
  }

  /// Sends an asynchronous message instead of pushing the data to the stream.
  /// Required for initiating handshakes (because no stream exists at that
  /// point) or for any other communication that bypasses the stream.
  template <class... Ts>
  void async_send(const caf::actor& receiver, Ts&&... xs) {
    BROKER_TRACE(BROKER_ARG(receiver)
                 << BROKER_ARG2("xs", std::forward_as_tuple(xs...)));
    self()->send(receiver, std::forward<Ts>(xs)...);
  }

  // Subscriptions use flooding.
  template <class... Ts>
  void send(const caf::actor& receiver, atom::subscribe atm, Ts&&... xs) {
    dref().async_send(receiver, atm, std::forward<Ts>(xs)...);
  }

  // Path revocations use flooding.
  template <class... Ts>
  void send(const caf::actor& receiver, atom::revoke atm, Ts&&... xs) {
    dref().async_send(receiver, atm, std::forward<Ts>(xs)...);
  }

  // Published messages use the stream.
  template <class T>
  void send(const caf::actor& receiver, atom::publish, T msg) {
    dref().stream_send(receiver, msg);
  }

  // -- peering ----------------------------------------------------------------

  // Initiates peering between A (this node) and B (remote peer).
  void start_peering(const peer_id_type& remote_peer, const caf::actor& hdl,
                     caf::response_promise rp) {
    BROKER_TRACE(BROKER_ARG(remote_peer) << BROKER_ARG(hdl));
    if (direct_connection(tbl(), remote_peer)) {
      BROKER_DEBUG("start_peering ignored: already peering with"
                   << remote_peer);
      rp.deliver(atom::peer_v, atom::ok_v, hdl);
    } else if (remote_peer < dref().id()) {
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
      s->request(hdl, std::chrono::minutes(10), atom::peer_v, dref().id(), s)
        .then(
          [](atom::peer, atom::ok, const peer_id_type&) {
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

  // Establishes a stream from B to A.
  caf::outbound_stream_slot<message_type, caf::actor, peer_id_type, filter_type,
                            lamport_timestamp>
  handle_peering_request(const peer_id_type& remote_peer,
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

  // Acks the stream from B to A and establishes a stream from A to B.
  caf::outbound_stream_slot<message_type, atom::ok, caf::actor, peer_id_type,
                            filter_type, lamport_timestamp>
  handle_peering_handshake_1(caf::stream<message_type>, const caf::actor& hdl,
                             const peer_id_type& remote_peer,
                             const filter_type& filter,
                             lamport_timestamp timestamp) {
    BROKER_TRACE(BROKER_ARG(hdl)
                 << BROKER_ARG(remote_peer) << BROKER_ARG(filter)
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

  // Acks the stream from A to B.
  void handle_peering_handshake_2(caf::stream<message_type> in, atom::ok,
                                  const caf::actor& hdl,
                                  const peer_id_type& remote_peer,
                                  const filter_type& filter,
                                  lamport_timestamp timestamp) {
    BROKER_TRACE(BROKER_ARG(hdl)
                 << BROKER_ARG(remote_peer) << BROKER_ARG(filter)
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

  void cleanup(handshake_ptr ptr) {
    pending_connections_.erase(ptr->remote_id);
    pending_slots_.erase(ptr->in);
    pending_slots_.erase(ptr->out);
  }

  void cleanup_and_replay_buffer_if_done(handshake_ptr ptr) {
    if (ptr->done()) {
      BROKER_DEBUG("completed handshake to" << ptr->remote_id);
      cleanup(ptr);
      ptr->replay_input_buffer();
    }
  }

  caf::stream_slot make_inbound_path(handshake_type* hs) {
    BROKER_TRACE(BROKER_ARG2("peer", hs->remote_id));
    auto res = dref().add_unchecked_inbound_path(caf::stream<message_type>{});
    BROKER_DEBUG("opened input slot" << res << "for peer" << hs->remote_id);
    pending_slots_.emplace(res, hs);
    return res;
  }

  caf::stream_slot make_outbound_path(handshake_type* hs) {
    BROKER_TRACE(BROKER_ARG2("peer", hs->remote_id));
    auto& d = dref();
    auto result = caf::invalid_stream_slot;
    auto self_hdl = caf::actor_cast<caf::actor>(self());
    if (hs->is_originator()) {
      auto data = std::make_tuple(atom::ok_v, std::move(self_hdl), d.id(),
                                  d.filter(), d.timestamp());
      result = d.template add_unchecked_outbound_path<message_type>(
        hs->remote_hdl, std::move(data));
    } else {
      BROKER_ASSERT(hs->is_responder());
      auto data = std::make_tuple(std::move(self_hdl), d.id(), d.filter(),
                                  d.timestamp());
      result = d.template add_unchecked_outbound_path<message_type>(
        hs->remote_hdl, std::move(data));
    }
    BROKER_ASSERT(result != caf::invalid_stream_slot);
    BROKER_DEBUG("opened output slot" << result << "for peer" << hs->remote_id);
    out_.template assign<typename peer_trait::manager>(result);
    pending_slots_.emplace(result, hs);
    return result;
  }

  using caf::stream_manager::finalize;

  bool finalize(handshake_type* hs) {
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
        dref().peer_discovered(hs->remote_id);
      dref().peer_connected(hs->remote_id, hs->remote_hdl);
      auto path = std::vector<peer_id_type>{hs->remote_id};
      auto path_ts = vector_timestamp{hs->remote_timestamp};
      dref().handle_filter_update(path, path_ts, hs->remote_filter);
      return true;
    }
  }

  void handle_buffered_msg(caf::inbound_path*, caf::outbound_path*,
                           message_type& msg) {
    handle_publication(msg);
  }

  void handle_buffered_msg(caf::inbound_path*, caf::outbound_path*,
                           caf::message& msg) {
    // TODO: we can make this more efficient with CAF 0.18 typed views.
    if (msg.match_elements<peer_id_list, vector_timestamp, filter_type>()) {
      handle_filter_update(msg.get_mutable_as<peer_id_list>(0),
                           msg.get_mutable_as<vector_timestamp>(1),
                           msg.get_as<filter_type>(2));
    } else if (msg.match_elements<peer_id_list, vector_timestamp, peer_id_type,
                                  filter_type>()) {
      handle_path_revocation(msg.get_mutable_as<peer_id_list>(0),
                             msg.get_mutable_as<vector_timestamp>(1),
                             msg.get_as<peer_id_type>(2),
                             msg.get_as<filter_type>(3));
    } else {
      BROKER_ERROR("unexpected buffered message:" << msg);
    }
  }

  template <class DownstreamMsg>
  std::enable_if_t<std::is_same<typename DownstreamMsg::outer_type,
                                caf::downstream_msg>::value>
  handle_buffered_msg(caf::inbound_path* path, caf::outbound_path*,
                      DownstreamMsg& msg) {
    handle(path, msg);
  }

  template <class UpstreamMsg>
  std::enable_if_t<std::is_same<typename UpstreamMsg::outer_type,
                                caf::upstream_msg>::value>
  handle_buffered_msg(caf::inbound_path*, caf::outbound_path* path,
                      UpstreamMsg& msg) {
    handle(path->slots.invert(), msg);
  }

  // -- callbacks --------------------------------------------------------------

  /// Pushes `msg` to local workers.
  void ship_locally(const data_message& msg) {
    if (!worker_manager().paths().empty())
      worker_manager().push(msg);
    super::ship_locally(msg);
  }

  /// Pushes `msg` to local stores.
  void ship_locally(const command_message& msg) {
    if (!store_manager().paths().empty())
      store_manager().push(msg);
    super::ship_locally(msg);
  }

  void shutdown(shutdown_options options) {
    BROKER_TRACE(BROKER_ARG(options));
    if (!store_manager().paths().empty()) {
      // TODO: honor wait-for-stores flag.
      auto cancel = make_error(ec::shutting_down);
      auto xs = store_manager().open_path_slots();
      for (auto x : xs)
        out_.remove_path(x, cancel, false);
    }
    if (auto peers = dref().peer_ids(); !peers.empty()) {
      for (auto& x : peers)
        unpeer(x);
    }
    tearing_down_ = true;
    continuous(false);
    super::shutdown(options);
  }

  // -- "overridden" member functions of alm::peer -----------------------------

  void handle_filter_update(peer_id_list& path, vector_timestamp& path_ts,
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

  void handle_path_revocation(peer_id_list& path, vector_timestamp path_ts,
                              const peer_id_type& revoked_hop,
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
  bool congested(const caf::inbound_path& path) const noexcept override {
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

  void handle(caf::inbound_path* path,
              caf::downstream_msg::batch& batch) override {
    BROKER_TRACE(BROKER_ARG(path) << BROKER_ARG(batch));
    if (auto hs = pending_slot(path->slots.receiver); hs == nullptr) {
      using peer_batch = typename peer_trait::batch;
      if (batch.xs.template match_elements<peer_batch>()) {
        for (auto& x : batch.xs.template get_mutable_as<peer_batch>(0))
          dref().handle_publication(x);
        return;
      }
      auto try_publish = [&, this](auto trait) {
        using batch_type = typename decltype(trait)::batch;
        if (batch.xs.template match_elements<batch_type>()) {
          for (auto& x : batch.xs.template get_mutable_as<batch_type>(0))
            dref().publish(x);
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

  void handle(caf::inbound_path* path, caf::downstream_msg::close& x) override {
    BROKER_TRACE(BROKER_ARG(path) << BROKER_ARG(x));
    if (auto hs = pending_slot(path->slots.receiver); hs == nullptr)
      handle_impl(path, x);
    else
      hs->input_buffer_emplace(std::move(x));
  }

  void handle(caf::inbound_path* path,
              caf::downstream_msg::forced_close& x) override {
    BROKER_TRACE(BROKER_ARG(path) << BROKER_ARG(x));
    if (auto hs = pending_slot(path->slots.receiver); hs == nullptr)
      handle_impl(path, x);
    else
      hs->input_buffer_emplace(std::move(x));
  }

  void handle(caf::stream_slots slots,
              caf::upstream_msg::ack_batch& x) override {
    BROKER_TRACE(BROKER_ARG(slots) << BROKER_ARG(x));
    if (auto hs = pending_slot(slots.receiver); hs == nullptr)
      caf::stream_manager::handle(slots, x);
    else
      hs->input_buffer_emplace(std::move(x));
  }

  void handle(caf::stream_slots slots, caf::upstream_msg::drop& x) override {
    BROKER_TRACE(BROKER_ARG(slots) << BROKER_ARG(x));
    if (auto hs = pending_slot(slots.receiver); hs == nullptr)
      handle_impl(slots, x);
    else
      hs->input_buffer_emplace(std::move(x));
  }

  void handle(caf::stream_slots slots,
              caf::upstream_msg::forced_drop& x) override {
    BROKER_TRACE(BROKER_ARG(slots) << BROKER_ARG(x));
    if (auto hs = pending_slot(slots.receiver); hs == nullptr)
      handle_impl(slots, x);
    else
      hs->input_buffer_emplace(std::move(x));
  }

  bool handle(caf::stream_slots slots,
              caf::upstream_msg::ack_open& x) override {
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

  bool done() const override {
    return !continuous() && pending_handshakes_ == 0 && inbound_paths_.empty()
           && out_.clean();
  }

  bool idle() const noexcept override {
    // Same as `stream_stage<...>`::idle().
    return out_.stalled() || (out_.clean() && this->inbound_paths_idle());
  }

  downstream_manager_type& out() override {
    return out_;
  }

  template <class... Fs>
  caf::behavior make_behavior(Fs... fs) {
    using detail::lift;
    auto& d = dref();
    return super::make_behavior(
      // Forward additional message handlers.
      std::move(fs)...,
      // Expose to member functions to messaging API.
      lift<atom::peer, atom::init>(d, &Derived::handle_peering_request),
      lift<>(d, &Derived::handle_peering_handshake_1),
      lift<>(d, &Derived::handle_peering_handshake_2),
      lift<atom::join>(d, &Derived::add_worker),
      lift<atom::join>(d, &Derived::add_sending_worker),
      lift<atom::join, atom::store>(d, &Derived::add_sending_store),
      // Trigger peering to remotes.
      [this](atom::peer, const peer_id_type& remote_peer,
             const caf::actor& hdl) {
        dref().start_peering(remote_peer, hdl, self()->make_response_promise());
      },
      // Per-stream subscription updates.
      [this](atom::join, atom::update, caf::stream_slot slot,
             filter_type& filter) {
        dref().subscribe(filter);
        worker_manager().set_filter(slot, filter);
      },
      [this](atom::join, atom::update, caf::stream_slot slot,
             filter_type& filter, const caf::actor& listener) {
        dref().subscribe(filter);
        worker_manager().set_filter(slot, filter);
        self()->send(listener, true);
      },
      // Allow local publishers to hook directly into the stream.
      [this](caf::stream<data_message> in) { add_unchecked_inbound_path(in); },
      [this](caf::stream<node_message_content> in) {
        add_unchecked_inbound_path(in);
      },
      // Special handlers for bypassing streams and/or forwarding.
      [this](atom::publish, atom::local, data_message& msg) {
        publish_locally(msg);
      },
      [this](atom::unpeer, const caf::actor& hdl) { dref().unpeer(hdl); },
      [this](atom::unpeer, const peer_id_type& peer_id) {
        dref().unpeer(peer_id);
      });
  }

protected:
  /// Disconnects a peer by demand of the user.
  void unpeer(const peer_id_type& peer_id, const caf::actor& hdl) {
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
      dref().peer_removed(peer_id, hdl);
    else
      dref().cannot_remove_peer(peer_id);
  }

  /// Disconnects a peer by demand of the user.
  void unpeer(const peer_id_type& peer_id) {
    BROKER_TRACE(BROKER_ARG(peer_id));
    if (auto i = tbl().find(peer_id); i != tbl().end()) {
      auto hdl = i->second.hdl;
      dref().unpeer(peer_id, hdl);
    } else if (auto hs = pending_connection(peer_id); hs != nullptr) {
      auto hdl = hs->remote_hdl;
      dref().unpeer(peer_id, hdl);
    } else {
      dref().cannot_remove_peer(peer_id);
    }
  }

  /// Disconnects a peer by demand of the user.
  void unpeer(const caf::actor& hdl) {
    BROKER_TRACE(BROKER_ARG(hdl));
    if (auto peer_id = get_peer_id(tbl(), hdl)) {
      dref().unpeer(*peer_id, hdl);
    } else if (auto hs = pending_connection(hdl); hs != nullptr) {
      auto peer_id = hs->remote_id;
      dref().unpeer(peer_id, hdl);
    } else {
      dref().cannot_remove_peer(hdl);
    }
  }

  /// Disconnects a peer as a result of receiving a `drop`, `forced_drop`,
  /// `close`, or `force_close` message.
  template <class Cause>
  void disconnect(const caf::actor& hdl, const error& reason, const Cause&) {
    BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(reason));
    hdl_to_slot_map* affected_map;
    hdl_to_slot_map* complementary_map;
    if constexpr (std::is_same<typename Cause::outer_type,
                               caf::downstream_msg>::value) {
      affected_map = &hdl_to_istream_;
      complementary_map = &hdl_to_ostream_;
    } else {
      affected_map = &hdl_to_ostream_;
      complementary_map = &hdl_to_istream_;
    }
    if (auto hs = pending_connection(hdl); hs != nullptr) {
      BROKER_DEBUG("lost peer" << hs->remote_id << "during handshake");
      hs->fail(ec::peer_disconnect_during_handshake);
      cleanup(std::move(hs));
    } else if (auto peer_id = get_peer_id(tbl(), hdl); !peer_id) {
      BROKER_DEBUG("unable to resolve handle via routing table");
    } else if (affected_map->erase(hdl) > 0) {
      // Call peer_disconnected only after in- and outbound paths were closed.
      if (complementary_map->count(hdl) == 0)
        dref().peer_disconnected(*peer_id, hdl, reason);
    } else {
      // Not an error. Usually caused by receiving a drop/close message after
      // we've already cleared up the state for some other reason.
      BROKER_DEBUG("path already closed");
    }
  }

  template <class Cause>
  void handle_impl(caf::inbound_path* path, Cause& cause) {
    BROKER_TRACE(BROKER_ARG(cause));
    if (path->hdl == nullptr) {
      BROKER_ERROR("closed inbound path with invalid communication handle");
    } else {
      auto hdl = caf::actor_cast<caf::actor>(path->hdl);
      if constexpr (std::is_same<caf::downstream_msg::close, Cause>::value) {
        error dummy;
        disconnect(hdl, dummy, cause);
      } else {
        disconnect(hdl, cause.reason, cause);
      }
    }
    // Reset the actor handle to make sure no further messages travel upstream.
    path->hdl = nullptr;
  }

  template <class Cause>
  void handle_impl(caf::stream_slots slots, Cause& cause) {
    BROKER_TRACE(BROKER_ARG(slots) << BROKER_ARG(cause));
    auto path = out_.path(slots.receiver);
    if (!path || path->hdl == nullptr) {
      BROKER_DEBUG("closed outbound path with invalid communication handle");
    } else {
      auto hdl = caf::actor_cast<caf::actor>(path->hdl);
      if constexpr (std::is_same<caf::upstream_msg::drop, Cause>::value) {
        error dummy;
        disconnect(hdl, dummy, cause);
        out_.remove_path(slots.receiver, dummy, false);
      } else {
        disconnect(hdl, cause.reason, cause);
        out_.remove_path(slots.receiver, cause.reason, true);
      }
    }
  }

  /// Organizes downstream communication to peers as well as local subscribers.
  downstream_manager_type out_;

  /// Maps communication handles to output slots.
  hdl_to_slot_map hdl_to_ostream_;

  /// Maps communication handles to input slots.
  hdl_to_slot_map hdl_to_istream_;

  /// Stores in-flight peering handshakes.
  std::unordered_map<peer_id_type, handshake_ptr> pending_connections_;

  /// Maps stream slots that still need to wait for the handshake to complete.
  std::unordered_map<caf::stream_slot, handshake_ptr> pending_slots_;

  /// Stores whether the `shutdown` callback was invoked.
  bool tearing_down_ = false;

private:
  Derived& dref() {
    return static_cast<Derived&>(*this);
  }
};

} // namespace broker::alm
