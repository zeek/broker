#ifndef BROKER_STORE_DETAIL_MASTER_ACTOR_HH
#define BROKER_STORE_DETAIL_MASTER_ACTOR_HH

#include <unordered_set>
#include <unordered_map>

#include <caf/stateful_actor.hpp>

#include "broker/fwd.hh"

namespace broker {
namespace store {
namespace detail {

struct master_state {
  std::unordered_set<caf::actor_addr> clones;
  std::unordered_map<data, data> backend;
  count sequence_number = 0;  // tracks mutating operations
};

caf::behavior master_actor(caf::stateful_actor<master_state>* self,
                           caf::actor core, std::string name);

} // namespace detail
} // namespace store
} // namespace broker

#endif // BROKER_STORE_DETAIL_MASTER_ACTOR_HH
