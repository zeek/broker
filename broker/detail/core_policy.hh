#ifndef BROKER_DETAIL_CORE_POLICY_HH
#define BROKER_DETAIL_CORE_POLICY_HH

#include <vector>
#include <utility>

#include <caf/actor.hpp>
#include <caf/actor_addr.hpp>
#include <caf/fwd.hpp>
#include <caf/message.hpp>

#include <caf/broadcast_topic_scatterer.hpp>
#include <caf/stream_id.hpp>

#include "broker/data.hh"
#include "broker/internal_command.hh"
#include "broker/topic.hh"

#include "broker/detail/filter_type.hh"
#include "broker/detail/peer_filter.hh"

namespace broker {
namespace detail {

struct core_state;

/// Sets up a configurable stream manager to act as a distribution tree for
/// Broker.
class core_policy {
public:
  // -- member types -----------------------------------------------------------

  /// Configures the input policy in use.
  using gatherer_type = caf::random_gatherer;

  /// Type to store a TTL for messages forwarded to peers.
  using ttl = uint16_t;

  /// A batch received from another peer.
  using peer_batch = std::vector<caf::message>;

  /// A batch received from local actors for regular workers.
  using worker_batch = std::vector<std::pair<topic, data>>;

  /// A batch received from local actors for store masters and clones.
  using store_batch = std::vector<std::pair<topic, internal_command>>;

  /// Identifier for a single up- or downstream path.
  using path_id = std::pair<caf::stream_id, caf::actor_addr>;

  /// Maps actor handles to path IDs.
  using peer_to_path_map = std::map<caf::actor, path_id>;

  /// Maps path IDs to actor handles.
  using path_to_peer_map = std::map<path_id, caf::actor>;

  /// Scatterer for dispatching data to remote peers.
  using main_stream_t = caf::broadcast_topic_scatterer<caf::message,
                                                       peer_filter,
                                                       peer_filter_matcher>;

  /// Scatterer for dispatching data to local actors (workers or stores).
  template <class T>
  using substream_t = caf::broadcast_topic_scatterer<std::pair<topic, T>,
                                                     filter_type,
                                                     prefix_matcher>;

  /// Composed scatterer type for bundled dispatching.
  using scatterer_type = caf::fused_scatterer<main_stream_t, substream_t<data>,
                                              substream_t<internal_command>>;

  core_policy(caf::detail::stream_distribution_tree<core_policy>* parent,
              core_state* state, filter_type filter);

  /// Returns true if 1) `shutting_down()`, 2) there is no more
  /// active local data source, and 3) there is no pending data to any peer.
  bool at_end() const;

  bool substream_local_data() const;

  void before_handle_batch(const caf::stream_id&, const caf::actor_addr& hdl,
                           long, caf::message&, int64_t);

  void handle_batch(caf::message& xs);

  void after_handle_batch(const caf::stream_id&, const caf::actor_addr&,
                          int64_t);

  void ack_open_success(const caf::stream_id& sid,
                        const caf::actor_addr& rebind_from,
                        caf::strong_actor_ptr rebind_to);

  void ack_open_failure(const caf::stream_id& sid,
                        const caf::actor_addr& rebind_from,
                        caf::strong_actor_ptr rebind_to, const caf::error&);

  void push_to_substreams(std::vector<caf::message> vec);

  caf::optional<caf::error> batch(const caf::stream_id&, const caf::actor_addr&,
                                  long, caf::message& xs, int64_t);

  // -- status updates to the state --------------------------------------------

  void peer_lost(const caf::actor& hdl);

  void peer_removed(const caf::actor& hdl);

  // -- callbacks for close/drop events ----------------------------------------

  caf::error path_closed(const caf::stream_id& sid, const caf::actor_addr& hdl);

  caf::error path_force_closed(const caf::stream_id& sid,
                               const caf::actor_addr& hdl, caf::error reason);

  caf::error path_dropped(const caf::stream_id& sid,
                          const caf::actor_addr& hdl);

  caf::error path_force_dropped(const caf::stream_id& sid,
                                const caf::actor_addr& hdl, caf::error reason);

  // -- state required by the distribution tree --------------------------------

  bool shutting_down() const;

  void shutting_down(bool value);

  // -- peer management --------------------------------------------------------
  
  /// Queries whether `hdl` is a known peer.
  bool has_peer(const caf::actor& hdl) const;

  /// Adds a new peer that isn't fully initialized yet. A peer is fully
  /// initialized if there is an upstream ID associated to it.
  bool add_peer(const caf::stream_id& sid,
                const caf::strong_actor_ptr& downstream_handle,
                const caf::actor& peer_handle, filter_type filter);

  /// Fully initializes a peer by setting an upstream ID and inserting it into
  /// the `input_to_peer_`  map.
  bool init_peer(const caf::stream_id& sid,
                 const caf::strong_actor_ptr& upstream_handle,
                 const caf::actor& peer_handle);

  /// Removes a peer, aborting any stream to & from that peer.
  bool remove_peer(const caf::actor& hdl, caf::error reason, bool silent,
                   bool graceful_removal);

  /// Updates the filter of an existing peer.
  bool update_peer(const caf::actor& hdl, filter_type filter);

  // -- selectively pushing data into the streams ------------------------------

  /// Pushes data to workers without forwarding it to peers.
  void local_push(topic x, data y);

  /// Pushes data to stores without forwarding it to peers.
  void local_push(topic x, internal_command y);

  /// Pushes data to peers only without forwarding it to local substreams.
  void remote_push(caf::message msg);

  /// Pushes data to peers and workers.
  void push(topic x, data y);

  /// Pushes data to peers and stores.
  void push(topic x, internal_command y);

  // -- state accessors --------------------------------------------------------

  main_stream_t& peers();

  const main_stream_t& peers() const;

  substream_t<data>& workers();

  const substream_t<data>& workers() const;

  substream_t<internal_command>& stores();

  const substream_t<internal_command>& stores() const;

  template <class F>
  void for_each_peer(F f) {
    // visit all peers that have at least one path still connected
    auto peers = get_peer_handles();
    std::for_each(peers.begin(), peers.end(), std::move(f));
  }

  std::vector<caf::actor> get_peer_handles();

  template <class Predicate>
  caf::actor find_output_peer_hdl(Predicate pred) {
    for (auto& kvp : peer_to_opath_)
      if (pred(kvp.first))
        return kvp.first;
    return nullptr;
  }

  template <class F>
  void for_each_filter(F f) {
    for (auto& kvp : peers().lanes()) {
      f(kvp.first.second);
    }
  }

  template <class F>
  void for_each_peer_filter(F f) {
    // This is a three-step process:
    // 1. Iterate all peer handles
    // 2. Get the path for each peer because the path handle can differ from
    //    the peer handle
    // 3. Select the lane for this peer and extract the filter.
    using lane_kvp = typename main_stream_t::lanes_map::value_type;
    auto& lanes = peers().lanes();
    auto e = lanes.end();
    // #1
    for (auto& kvp : peer_to_opath_) {
      // #2
      auto path = peers().find(kvp.second.first, kvp.second.second);
      if (!path)
        continue;
      // #3
      auto i = std::find_if(lanes.begin(), e, [&](const lane_kvp& x) {
        return x.first.first == path->hdl;
      });
      if (i == e)
        continue;
      f(kvp.first, i->first.second);
    }
  }

private:
  /// Pointer to the parent.
  caf::detail::stream_distribution_tree<core_policy>* parent_;

  /// Pointer to the state.
  core_state* state_;

  /// Maps peer handles to output path IDs.
  peer_to_path_map peer_to_opath_;

  /// Maps output path IDs to peer handles.
  path_to_peer_map opath_to_peer_;

  /// Maps peer handles to input path IDs.
  peer_to_path_map peer_to_ipath_;

  /// Maps input path IDs to peer handles.
  path_to_peer_map ipath_to_peer_;
};

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_CORE_POLICY_HH
