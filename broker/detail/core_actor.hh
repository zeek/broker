#ifndef BROKER_DETAIL_CORE_ACTOR_HH
#define BROKER_DETAIL_CORE_ACTOR_HH

#include <unordered_set>
#include <unordered_map>
#include <map>
#include <vector>

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

#include "broker/endpoint_info.hh"
#include "broker/optional.hh"
#include "broker/network_info.hh"
#include "broker/peer_info.hh"

#include "broker/detail/radix_tree.hh"
#include "broker/detail/stream_governor.hh"
#include "broker/detail/stream_relay.hh"

namespace broker {
namespace detail {

struct core_state {
  // --- nested types ----------------------------------------------------------

  /// Identifies the two individual streams forming a bidirectional channel.
  /// The first ID denotes the *input*  and the second ID denotes the *output*.
  using stream_id_pair = std::pair<caf::stream_id, caf::stream_id>;

  // --- construction ----------------------------------------------------------

  /// Establishes all invariants.
  void init(caf::event_based_actor* s, filter_type initial_filter);

  // --- message introspection -------------------------------------------------

  /// Returns the peer that sent the current message.
  /// @pre `xs.match_elements<stream_msg>()`
  caf::strong_actor_ptr prev_peer_from_handshake();

  // --- filter management -----------------------------------------------------

  /// Sends the current filter to all peers.
  void update_filter_on_peers();

  /// Adds `xs` to our filter and update all peers on changes.
  void add_to_filter(filter_type xs);

  // --- convenience functions for querying state ------------------------------

  /// Returns whether `x` is either a pending peer or a connected peer.
  bool has_peer(const caf::actor& x);

  /// Returns whether a master for `name` probably exists already on one of our
  /// peers.
  bool has_remote_master(const std::string& name);

  // --- member variables ------------------------------------------------------

  /// Stores all master actors created by this core.
  std::unordered_map<std::string, caf::actor> masters;

  /// Stores all clone actors created by this core.
  std::unordered_multimap<std::string, caf::actor> clones;

  /// Requested topics on this core.
  filter_type filter;
 
  /// Multiplexes local streams and streams for peers.
  detail::stream_governor_ptr governor;

  /// Maps pending peer handles to output IDs.
  std::unordered_map<caf::actor, caf::stream_id> pending_peers;

  /// Points to the owning actor.
  caf::event_based_actor* self;

  /// Connects the governor to the input of local actor.
  caf::stream_handler_ptr worker_relay;

  /// Connects the governor to the input of local actor.
  caf::stream_handler_ptr store_relay;

  /// Name shown in logs for all instances of this actor.
  static const char* name;
};

caf::behavior core_actor(caf::stateful_actor<core_state>* self,
                         filter_type initial_filter);

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_CORE_ACTOR_HH
