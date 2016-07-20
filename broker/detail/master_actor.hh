#ifndef BROKER_DETAIL_MASTER_ACTOR_HH
#define BROKER_DETAIL_MASTER_ACTOR_HH

#include <unordered_set>

#include <caf/stateful_actor.hpp>

#include "broker/fwd.hh"

namespace broker {
namespace detail {

class abstract_backend;

struct master_state {
  std::unique_ptr<abstract_backend> backend;
  std::unordered_set<caf::actor_addr> clones;
  count sequence_number = 0;  // tracks mutating operations
};

caf::behavior master_actor(caf::stateful_actor<master_state>* self,
                           caf::actor core, std::string name,
                           std::unique_ptr<abstract_backend> backend);

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_MASTER_ACTOR_HH
