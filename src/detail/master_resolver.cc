#include "broker/logger.hh" // Must come before any CAF include.

#include <utility>
#include <string>
#include <vector>
#include <caf/actor.hpp>
#include <caf/behavior.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/event_based_actor.hpp>

#include "broker/atoms.hh"
#include "broker/error.hh"

#include "broker/detail/master_resolver.hh"

namespace broker {
namespace detail {

caf::behavior master_resolver(caf::stateful_actor<master_resolver_state>* self) {
  self->set_error_handler([=](error&) {
    if (--self->state.remaining_responses == 0) {
      CAF_LOG_DEBUG("resolver failed to find a master");
      self->send(self->state.who_asked, atom::master::value,
                 make_error(ec::no_such_master, "no master on peers"));
      self->quit();
    }
  });
  return {
    [=](const std::vector<caf::actor>& peers, const std::string& name,
        caf::actor& who_asked) {
      CAF_LOG_DEBUG("resolver starts looking for:" << name);

      for (auto& peer : peers)
        self->send(peer, atom::store::value, atom::master::value,
                   atom::get::value, name);

      self->state.remaining_responses = peers.size();
      self->state.who_asked = std::move(who_asked);
    },
    [=](caf::actor& master) {
      CAF_LOG_DEBUG("resolver found master:" << master);
      self->send(self->state.who_asked, atom::master::value,
                 std::move(master));
      self->quit();
    }
  };
}

} // namespace detail
} // namespace broker
