#pragma once

#include <unordered_map>
#include <vector>

#include <caf/broadcast_downstream_manager.hpp>
#include <caf/cow_tuple.hpp>
#include <caf/detail/scope_guard.hpp>
#include <caf/detail/unordered_flat_map.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/flow/merge.hpp>
#include <caf/fused_downstream_manager.hpp>

#include "broker/alm/peer.hh"
#include "broker/alm/routing_table.hh"
#include "broker/detail/hash.hh"
#include "broker/detail/lift.hh"
#include "broker/detail/peer_handshake.hh"
#include "broker/detail/prefix_matcher.hh"
#include "broker/detail/unipath_manager.hh"
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
class stream_transport : public peer, public detail::unipath_manager::observer {
public:
  // -- member types -----------------------------------------------------------

  using super = peer;

  using endpoint_id_list = std::vector<endpoint_id>;

  /// Maps peer actor handles to their stream managers.
  using hdl_to_mgr_map
    = std::unordered_map<caf::actor, detail::peer_manager_ptr>;

  /// Maps stream managers to peer actor handles.
  using mgr_to_hdl_map
    = std::unordered_map<detail::peer_manager_ptr, caf::actor>;

  // -- constructors, destructors, and assignment operators --------------------

  explicit stream_transport(caf::event_based_actor* self);

  // -- properties -------------------------------------------------------------

  /// Returns whether this manager has connection to `hdl`.`
  [[nodiscard]] bool connected_to(const caf::actor& hdl) const noexcept;

  // -- adding local subscribers -----------------------------------------------

  /// Subscribes `current_sender()` to @p data_message events that match
  /// @p filter.
  /// @pre `current_sender() != nullptr`
  caf::outbound_stream_slot<data_message>
  add_sending_worker(filter_type filter);

  /// Subscribes @p hdl to @p data_message events that match @p filter.
  error add_worker(const caf::actor& hdl, filter_type filter);

  /// Subscribes `current_sender()` to @p command_message events that match
  /// @p filter.
  /// @pre `current_sender() != nullptr`
  caf::outbound_stream_slot<command_message>
  add_sending_store(filter_type filter);

  /// Subscribes @p hdl to @p command_message events that match @p filter.
  error add_store(const caf::actor& hdl, filter_type filter);

  // -- overrides for peer::publish --------------------------------------------

  void publish(const caf::actor& dst, atom::subscribe,
               const endpoint_id_list& path, const vector_timestamp& ts,
               const filter_type& new_filter) override;

  void publish(const caf::actor& dst, atom::revoke,
               const endpoint_id_list& path, const vector_timestamp& ts,
               const endpoint_id& lost_peer,
               const filter_type& new_filter) override;

  using super::publish_locally;

  void publish_locally(const data_message& msg) override;

  void publish_locally(const command_message& msg) override;

  // -- peering ----------------------------------------------------------------

  detail::peer_manager_ptr
  get_or_insert_pending(const endpoint_id& remote_peer);

  detail::peer_manager_ptr get_pending(const caf::actor& hdl);

  detail::peer_manager_ptr get_pending(const endpoint_id& remote_peer);

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

  // void cleanup(detail::peer_handshake_ptr ptr);

  // void cleanup_and_replay_buffer_if_done(detail::peer_handshake_ptr ptr);

  bool finalize(detail::peer_handshake* hs);

  // -- overrides for alm::peer ------------------------------------------------

  void shutdown(shutdown_options options) override;

  void handle_filter_update(endpoint_id_list& path, vector_timestamp& path_ts,
                            const filter_type& filter) override;

  void handle_path_revocation(endpoint_id_list& path, vector_timestamp& path_ts,
                              const endpoint_id& revoked_hop,
                              const filter_type& filter) override;

  // -- overrides for detail::central_dispatcher -------------------------------

  /// @private
  template <class T>
  void dispatch_impl(const T& msg);

  void dispatch(const data_message& msg) override;

  void dispatch(const command_message& msg) override;

  void dispatch(node_message&& msg) override;

  void flush() override;

  // -- overrides for detail::unipath_manager::observer ------------------------

  void closing(detail::unipath_manager* ptr, bool graceful,
               const error& reason) override;

  void downstream_connected(detail::unipath_manager* ptr,
                            const caf::actor& hdl) override;

  bool finalize_handshake(detail::peer_manager*) override;

  void abort_handshake(detail::peer_manager*) override;

  // -- initialization ---------------------------------------------------------

  caf::behavior make_behavior() override;

protected:
  // -- utility ----------------------------------------------------------------

  /// Updates the filter of a data or command sink.
  bool update_filter(caf::stream_slot slot, filter_type&& filter);

  /// Removes state for `hdl` and returns whether any cleanup steps were
  /// performed.
  /// @param reason When removed by user action `null`, otherwise a pointer to
  ///               the error that triggered the cleanup.
  bool peer_cleanup(const endpoint_id& peer_id, const error* reason = nullptr);

  /// Disconnects a peer as a result of an error.
  void drop_peer(const caf::actor& hdl, const error& reason);

  /// Disconnects a peer by demand of the user.
  void unpeer(const endpoint_id& peer_id, const caf::actor& hdl);

  /// Disconnects a peer by demand of the user.
  void unpeer(const endpoint_id& peer_id);

  /// Disconnects a peer by demand of the user.
  void unpeer(const caf::actor& hdl);

  /// Tries to find a peer manager for `peer_id` in pending_ or hdl_to_mgr_.
  detail::peer_manager* peer_lookup(const endpoint_id& peer_id);

  /// Maps peer handles to their respective unipath manager.
  hdl_to_mgr_map hdl_to_mgr_;

  /// Maps unipath managers to their respective peer handle.
  mgr_to_hdl_map mgr_to_hdl_;

  /// Stores connections to peer that yet have to finish the handshake.
  std::unordered_map<endpoint_id, detail::peer_manager_ptr, detail::fnv>
    pending_;

  /// Stores local data message subscribers .
  std::vector<detail::unipath_data_sink_ptr> data_sinks_;

  /// Stores local command message subscribers .
  std::vector<detail::unipath_command_sink_ptr> command_sinks_;

  caf::flow::merger_impl_ptr<data_message> data_inputs_;

  caf::flow::merger_impl_ptr<command_message> command_inputs_;

  caf::flow::merger_impl_ptr<packed_message> central_merge_;
};

} // namespace broker::alm
