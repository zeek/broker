#include "broker/detail/store_actor.hh"

namespace broker::detail {

void store_actor_state::init(caf::event_based_actor* self,
                             endpoint::clock* clock, std::string&& id,
                             caf::actor&& core) {
  BROKER_ASSERT(self != nullptr);
  BROKER_ASSERT(clock != nullptr);
  this->self = self;
  this->clock = clock;
  this->id = std::move(id);
  this->core = std::move(core);
}

} // namespace broker::detail
