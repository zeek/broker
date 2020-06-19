#pragma once

#include <fstream>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

#include "broker/alm/stream_transport.hh"
#include "broker/atoms.hh"
#include "broker/configuration.hh"
#include "broker/detail/network_cache.hh"
#include "broker/detail/radix_tree.hh"
#include "broker/endpoint.hh"
#include "broker/endpoint_info.hh"
#include "broker/error.hh"
#include "broker/filter_type.hh"
#include "broker/logger.hh"
#include "broker/network_info.hh"
#include "broker/optional.hh"
#include "broker/peer_info.hh"
#include "broker/status.hh"

namespace broker {

class core_manager : public alm::stream_transport<core_manager, caf::node_id> {
public:
  // --- member types ----------------------------------------------------------

  using super = alm::stream_transport<core_manager, caf::node_id>;

  /// Identifies the two individual streams forming a bidirectional channel.
  /// The first ID denotes the *input*  and the second ID denotes the
  /// *output*.
  using stream_id_pair = std::pair<caf::stream_slot, caf::stream_slot>;

  // --- construction ----------------------------------------------------------

  core_manager(caf::event_based_actor* ptr, const filter_type& filter,
               broker_options opts, endpoint::clock* ep_clock);

  // --- filter management -----------------------------------------------------

  /// Sends the current filter to all peers.
  void update_filter_on_peers();

  /// Adds `xs` to our filter and update all peers on changes.
  void add_to_filter(filter_type xs);

  /// Returns the filter for `hdl`
  /// @pre `has_peer(hdl)`
  peer_filter& get_filter(const caf::actor& hdl);
  
  /// Returns a joint filter that contains all filter of neighbors and the filter of the local node
  filter_type get_all_filter();

  /// Returns a joint filter that contains all filter of neighbors and the filter of the local node
  /// except for the given actor hdl
  filter_type get_all_filter(caf::actor& hdl);

  /// Returns a joint filter that contains all filter of neighbors and the filter of the local node
  /// except the filters in set skip
  filter_type get_all_filter(std::set<caf::actor>& skip);

  // --- convenience functions for querying state ------------------------------

  /// Returns whether `x` is either a pending peer or a connected peer.
  bool has_peer(const caf::actor& x);

  /// Returns whether a master for `name` probably exists already on one of
  /// our peers.
  bool has_remote_master(const std::string& name);
  
  // -- peer management  -----------------------------------------------------

  /// Updates the filter of an existing peer.
  bool update_peer(const caf::actor& hdl);
  bool update_peer(const caf::actor& hdl, filter_type filter);

  /// Updates the filter of all our neighbors
  bool update_routing(const caf::actor& hdl, filter_type filter);

  // --- convenience functions for sending errors and events -------------------

  template <ec ErrorCode>
  void emit_error(caf::actor hdl, const char* msg) {
    auto emit = [=](network_info x) {
      BROKER_INFO("error" << ErrorCode << x);
      // TODO: consider creating the data directly rather than going through the
      //       error object and converting it.
      auto err
        = make_error(ErrorCode, endpoint_info{hdl.node(), std::move(x)}, msg);
      this->local_push(make_data_message(topics::errors, get_as<data>(err)));
    };
    if (self()->node() != hdl.node())
      cache.fetch(
        hdl, [=](network_info x) { emit(std::move(x)); },
        [=](caf::error) { emit({}); });
    else
      emit({});
  }

  template <ec ErrorCode>
  void emit_error(caf::strong_actor_ptr hdl, const char* msg) {
    emit_error<ErrorCode>(caf::actor_cast<caf::actor>(hdl), msg);
  }

  template <ec ErrorCode>
  void emit_error(network_info inf, const char* msg) {
    auto x = cache.find(inf);
    if (x)
      emit_error<ErrorCode>(std::move(*x), msg);
    else {
      BROKER_INFO("error" << ErrorCode << inf);
      auto err
        = make_error(ErrorCode, endpoint_info{node_id(), std::move(inf)}, msg);
      local_push(make_data_message(topics::errors, get_as<data>(err)));
    }
  }

  template <sc StatusCode>
  void emit_status(caf::actor hdl, const char* msg) {
    static_assert(StatusCode != sc::peer_added,
                  "Use emit_peer_added_status instead");
    auto emit = [=](network_info x) {
      BROKER_INFO("status" << StatusCode << x);
      // TODO: consider creating the data directly rather than going through
      // the
      //       status object and converting it.
      auto stat = status::make<StatusCode>(
        endpoint_info{hdl.node(), std::move(x)}, msg);
      local_push(make_data_message(topics::statuses, get_as<data>(stat)));
    };
    if (self()->node() != hdl.node())
      cache.fetch(
        hdl, [=](network_info x) { emit(x); }, [=](caf::error) { emit({}); });
    else
      emit({});
  }

  void emit_peer_added_status(caf::actor hdl, const char* msg);

  template <sc StatusCode>
  void emit_status(caf::strong_actor_ptr hdl, const char* msg) {
    emit_status<StatusCode>(caf::actor_cast<caf::actor>(std::move(hdl)), msg);
  }

  void sync_with_status_subscribers(caf::actor new_peer);

  // --- member variables ------------------------------------------------------

  /// A copy of the current Broker configuration options.
  broker_options options;

  /// Stores all master actors created by this core.
  std::unordered_map<std::string, caf::actor> masters;

  /// Stores all clone actors created by this core.
  std::unordered_map<std::string, caf::actor> clones;

  /// Requested topics on this core.
  filter_type filter;

  /// Associates network addresses to remote actor handles and vice versa.
  detail::network_cache cache;

  /// Set to `true` after receiving a shutdown message from the endpoint.
  bool shutting_down;

  /// Required when spawning data stores.
  endpoint::clock* clock;

  /// Keeps track of all actors that subscribed to status updates.
  std::unordered_set<caf::actor> status_subscribers;

  /// Keeps track of all actors that currently wait for handshakes to
  /// complete.
  std::unordered_map<caf::actor, size_t> peers_awaiting_status_sync;

  /// Handle for recording all subscribed topics (if enabled).
  std::ofstream topics_file;

  /// Handle for recording all peers (if enabled).
  std::ofstream peers_file;
};

struct core_state {
  /// Establishes all invariants.
  void init(filter_type initial_filter, broker_options opts,
            endpoint::clock* ep_clock);

  /// Multiplexes local streams and streams for peers.
  caf::intrusive_ptr<core_manager> mgr;

  /// Gives this actor a recognizable name in log output.
  static inline const char* name = "core";
};

using core_actor_type = caf::stateful_actor<core_state>;

caf::behavior core_actor(core_actor_type* self, filter_type initial_filter,
                         broker_options opts, endpoint::clock* clock);

} // namespace broker
