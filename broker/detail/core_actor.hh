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

namespace broker {
namespace detail {

struct subscription_state {
  std::unordered_set<caf::actor> subscribers;
  uint64_t messages = 0;
};

struct peer_state {
  optional<caf::actor> actor;
  peer_info info;
};

struct core_state {
  /// Establishes all invariants.
  void init(caf::event_based_actor* s, filter_type initial_filter);

  /// Returns the peer that sent the current message.
  /// @pre `xs.match_elements<stream_msg>()`
  caf::strong_actor_ptr prev_peer_from_handshake();

  //std::vector<peer_state> peers;
  radix_tree<subscription_state> subscriptions;
  std::unordered_map<std::string, caf::actor> masters;
  std::unordered_multimap<std::string, caf::actor> clones;
  std::map<network_info, caf::actor> supervisors;
  endpoint_info info;

  /// Lists all known peers that we need to update whenever `filter` changes.
  std::vector<caf::strong_actor_ptr> peers;

  /// Requested topics on this core.
  filter_type filter;
 
  /// Multiplexes local streams and streams for peers.
  caf::intrusive_ptr<stream_governor> governor;

  /// Stream ID used by the governor.
  caf::stream_id sid;

  /// Set of pending handshake requests.
  std::unordered_set<caf::strong_actor_ptr> pending_peers;

  /// Points to the owning actor.
  caf::event_based_actor* self;

  /// Name shown in logs for all instances of this actor.
  static const char* name;
};

caf::behavior core_actor(caf::stateful_actor<core_state>* self,
                         filter_type initial_filter);

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_CORE_ACTOR_HH
