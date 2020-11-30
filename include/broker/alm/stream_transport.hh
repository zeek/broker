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
#include "broker/fwd.hh"
#include "broker/logger.hh"
#include "broker/message.hh"
#include "broker/shutdown_options.hh"

namespace broker::alm {

/// The transport registers these message handlers:
///
/// ~~~
/// (atom::peer, endpoint_id, actor) -> void
/// => start_peering(id, hdl)
///
/// (atom::peer, atom::init, endpoint_id, actor) -> slot
/// => handle_peering_request(...)
///
/// (stream<node_message>, actor, endpoint_id, filter_type, lamport_timestamp) -> slot
/// => handle_peering_handshake_1(...)
///
/// (stream<node_message>, actor, endpoint_id) -> void
/// => handle_peering_handshake_2(...)
///
/// (atom::unpeer, actor hdl) -> void
/// => disconnect(hdl)
/// ~~~
class stream_transport : public peer, public caf::stream_manager {
public:
  // -- member types -----------------------------------------------------------

  using super = peer;

  using endpoint_id_list = std::vector<endpoint_id>;

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
    using element = node_message;

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

  using worker_slot = caf::outbound_stream_slot<typename worker_trait::element>;

  using store_slot = caf::outbound_stream_slot<typename store_trait::element>;

  // -- imported member functions (silence hiding warnings) --------------------

  using caf::stream_manager::shutdown;

  // -- constructors, destructors, and assignment operators --------------------

  explicit stream_transport(caf::event_based_actor* self);

  // -- properties -------------------------------------------------------------

  /// Returns the slot for outgoing traffic to `hdl`.
  optional<caf::stream_slot> output_slot(const caf::actor& hdl) const noexcept;

  /// Returns the slot for incoming traffic from `hdl`.
  optional<caf::stream_slot> input_slot(const caf::actor& hdl) const noexcept;

  /// Returns whether this manager has inbound and outbound streams from and to
  /// `hdl`.`
  bool connected_to(const caf::actor& hdl) const noexcept;

  const auto& pending_connections() const noexcept {
    return pending_connections_;
  }

  /// Returns the handshake state for `peer_id` or `nullptr`.
  handshake_ptr pending_connection(const endpoint_id& peer_id) const noexcept;

  /// Like `pending_connection`, but inserts and returns a new handshake object
  /// if none exists yet.
  handshake_ptr pending_connection_or_insert(const endpoint_id& peer_id);

  /// Returns the handshake state for `peer_id` or `nullptr`.
  /// @note This overload has complexity `O(n)`.
  handshake_ptr pending_connection(const caf::actor& hdl) const noexcept;

  /// Returns the handshake state for `in` or `nullptr`.
  handshake_ptr pending_slot(caf::stream_slot in) const noexcept;

  peer_trait::manager& peer_manager() noexcept;

  worker_trait::manager& worker_manager() noexcept;

  store_trait::manager& store_manager() noexcept;

  caf::event_based_actor* self() noexcept {
    // We inherit this function from both base types. We are explicitly picking
    // one here for disambiguation.
    return super::self();
  }

  // -- adding local subscribers -----------------------------------------------

  /// Subscribes `self->current_sender()` to `worker_manager()`.
  worker_slot add_sending_worker(const filter_type& filter);

  /// Subscribes `hdl` to `worker_manager()`.
  error add_worker(const caf::actor& hdl, const filter_type& filter);

  /// Subscribes `self->sender()` to `store_manager()`.
  store_slot add_sending_store(const filter_type& filter);

  /// Subscribes `hdl` to `store_manager()`.
  error add_store(const caf::actor& hdl, const filter_type& filter);

  // -- sending ----------------------------------------------------------------

  void send(const caf::actor& receiver, atom::publish,
            node_message content) override;

  void send(const caf::actor& receiver, atom::subscribe,
            const endpoint_id_list& path, const vector_timestamp& ts,
            const filter_type& new_filter) override;

  void send(const caf::actor& receiver, atom::revoke,
            const endpoint_id_list& path, const vector_timestamp& ts,
            const endpoint_id& lost_peer,
            const filter_type& new_filter) override;

  // -- peering ----------------------------------------------------------------

  // Initiates peering between A (this node) and B (remote peer).
  void start_peering(const endpoint_id& remote_peer, const caf::actor& hdl,
                     caf::response_promise rp);

  // Establishes a stream from B to A.
  caf::outbound_stream_slot<node_message, caf::actor, endpoint_id, filter_type,
                            lamport_timestamp>
  handle_peering_request(const endpoint_id& remote_peer, const caf::actor& hdl);

  // Acks the stream from B to A and establishes a stream from A to B.
  caf::outbound_stream_slot<node_message, atom::ok, caf::actor, endpoint_id,
                            filter_type, lamport_timestamp>
  handle_peering_handshake_1(caf::stream<node_message>, const caf::actor& hdl,
                             const endpoint_id& remote_peer,
                             const filter_type& filter,
                             lamport_timestamp timestamp);

  // Acks the stream from A to B.
  void handle_peering_handshake_2(caf::stream<node_message> in, atom::ok,
                                  const caf::actor& hdl,
                                  const endpoint_id& remote_peer,
                                  const filter_type& filter,
                                  lamport_timestamp timestamp);

  void cleanup(handshake_ptr ptr);

  void cleanup_and_replay_buffer_if_done(handshake_ptr ptr);

  caf::stream_slot make_inbound_path(handshake_type* hs);

  caf::stream_slot make_outbound_path(handshake_type* hs);

  using caf::stream_manager::finalize;

  bool finalize(handshake_type* hs);

  void handle_buffered_msg(caf::inbound_path*, caf::outbound_path*,
                           node_message& msg);

  void handle_buffered_msg(caf::inbound_path*, caf::outbound_path*,
                           caf::message& msg);

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

  // -- overrides for alm::peer ------------------------------------------------

  void ship_locally(const data_message& msg) override;

  void ship_locally(const command_message& msg) override;

  void shutdown(shutdown_options options) override;

  void handle_filter_update(endpoint_id_list& path, vector_timestamp& path_ts,
                            const filter_type& filter) override;

  void handle_path_revocation(endpoint_id_list& path, vector_timestamp& path_ts,
                              const endpoint_id& revoked_hop,
                              const filter_type& filter) override;

  // -- overridden member functions of caf::stream_manager ---------------------

#if CAF_VERSION >= 1800
  bool congested(const caf::inbound_path& path) const noexcept override;
#endif

  void handle(caf::inbound_path* path,
              caf::downstream_msg::batch& batch) override;

  void handle(caf::inbound_path* path, caf::downstream_msg::close& x) override;

  void handle(caf::inbound_path* path,
              caf::downstream_msg::forced_close& x) override;

  void handle(caf::stream_slots slots,
              caf::upstream_msg::ack_batch& x) override;

  void handle(caf::stream_slots slots, caf::upstream_msg::drop& x) override;

  void handle(caf::stream_slots slots,
              caf::upstream_msg::forced_drop& x) override;

  bool handle(caf::stream_slots slots, caf::upstream_msg::ack_open& x) override;

  bool done() const override;

  bool idle() const noexcept override;

  downstream_manager_type& out() override;

  // -- initialization ---------------------------------------------------------

  caf::behavior make_behavior() override;

protected:
  // -- utility ----------------------------------------------------------------

  /// Disconnects a peer by demand of the user.
  void unpeer(const endpoint_id& peer_id, const caf::actor& hdl);

  /// Disconnects a peer by demand of the user.
  void unpeer(const endpoint_id& peer_id);

  /// Disconnects a peer by demand of the user.
  void unpeer(const caf::actor& hdl);

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
        peer_disconnected(*peer_id, hdl, reason);
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
  std::unordered_map<endpoint_id, handshake_ptr> pending_connections_;

  /// Maps stream slots that still need to wait for the handshake to complete.
  std::unordered_map<caf::stream_slot, handshake_ptr> pending_slots_;

  /// Stores whether the `shutdown` callback was invoked.
  bool tearing_down_ = false;
};

} // namespace broker::alm
