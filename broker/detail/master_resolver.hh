#ifndef BROKER_DETAIL_MASTER_RESOLVER_HH
#define BROKER_DETAIL_MASTER_RESOLVER_HH

#include <vector>

#include <caf/actor.hpp>
#include <caf/behavior.hpp>
#include <caf/stateful_actor.hpp>


namespace broker {
namespace detail {

struct master_resolver_state {
  size_t remaining_responses;
  caf::actor who_asked;
};

/// Queries each peer in `peers`.
caf::behavior master_resolver(caf::stateful_actor<master_resolver_state>* self);

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_MASTER_RESOLVER_HH
