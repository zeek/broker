#ifndef BROKER_DETAIL_CORE_ACTOR_HH
#define BROKER_DETAIL_CORE_ACTOR_HH

#include <unordered_set>
#include <unordered_map>
#include <map>
#include <vector>

#include <caf/actor.hpp>
#include <caf/stateful_actor.hpp>

#include "broker/api_flags.hh"
#include "broker/endpoint_info.hh"
#include "broker/optional.hh"
#include "broker/network_info.hh"
#include "broker/peer_info.hh"

#include "broker/detail/radix_tree.hh"

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
  bool routable;
  std::vector<peer_state> peers;
  radix_tree<subscription_state> subscriptions;
  std::unordered_map<std::string, caf::actor> masters;
  std::unordered_multimap<std::string, caf::actor> clones;
  std::map<network_info, caf::actor> supervisors;
  endpoint_info info;
  const char* name = "core";
};

caf::behavior core_actor(caf::stateful_actor<core_state>* self,
                         caf::actor subscriber, api_flags flags);

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_CORE_ACTOR_HH
