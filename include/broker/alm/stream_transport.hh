#pragma once

#include <unordered_map>
#include <vector>

#include <caf/cow_tuple.hpp>
#include <caf/detail/scope_guard.hpp>
#include <caf/detail/unordered_flat_map.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/flow/merge.hpp>

#include "broker/alm/peer.hh"
#include "broker/alm/routing_table.hh"
#include "broker/detail/connector.hh"
#include "broker/detail/connector_adapter.hh"
#include "broker/detail/flow_controller.hh"
#include "broker/detail/hash.hh"
#include "broker/detail/lift.hh"
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
class stream_transport : public peer, public detail::flow_controller {
public:
  // -- member types -----------------------------------------------------------

  using super = peer;

  // -- constructors, destructors, and assignment operators --------------------

  explicit stream_transport(caf::event_based_actor* self);

  stream_transport(caf::event_based_actor* self, detail::connector_ptr conn);

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

  void dispatch(const data_message& msg) override;

  void dispatch(const command_message& msg) override;

  void dispatch(const node_message& msg) override;

  // -- overrides for alm::peer ------------------------------------------------

  void shutdown(shutdown_options options) override;

  // -- overrides for flow_controller ------------------------------------------

  caf::scheduled_actor* ctx() override;

  void add_source(caf::flow::observable<data_message> source) override;

  void add_source(caf::flow::observable<command_message> source) override;

  void add_sink(caf::flow::observer<data_message> sink) override;

  void add_sink(caf::flow::observer<command_message> sink) override;

  caf::async::publisher<data_message>
  select_local_data(const filter_type& filter) override;

  caf::async::publisher<command_message>
  select_local_commands(const filter_type& filter) override;

  void add_filter(const filter_type& filter) override;

  // -- initialization ---------------------------------------------------------

  caf::behavior make_behavior() override;

protected:
  // -- utility ----------------------------------------------------------------

  caf::error init_new_peer(endpoint_id peer, alm::lamport_timestamp ts,
                           const filter_type& filter,
                           caf::net::stream_socket sock);

  /// Disconnects a peer by demand of the user.
  void unpeer(const endpoint_id& peer_id);

  /// Initialized the `data_outputs_` member lazily.
  void init_data_outputs();

  /// Initialized the `command_outputs_` member lazily.
  void init_command_outputs();

  /// Pushes events from the connector to the stream.
  caf::flow::buffered_observable_impl_ptr<data_message> connector_inputs_;

  /// Collects inputs from @ref broker::publisher objects.
  caf::flow::merger_impl_ptr<data_message> data_inputs_;

  /// Collects inputs from data store objects.
  caf::flow::merger_impl_ptr<command_message> command_inputs_;

  /// Provides central access to packed messages with routing information.
  caf::flow::merger_impl_ptr<node_message> central_merge_;

  /// Provides access to local @ref broker::subscriber objects.
  caf::flow::observable<data_message> data_outputs_;

  /// Provides access to local data store objects.
  caf::flow::observable<command_message> command_outputs_;

  /// Handle to the background worker for establishing peering relations.
  std::unique_ptr<detail::connector_adapter> connector_adapter_;

  /// Handles for aborting flows on unpeering.
  std::map<endpoint_id, caf::disposable> peers_;
};

} // namespace broker::alm
