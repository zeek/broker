#include "broker/logger.hh" // Must come before any CAF include.

#include <caf/all.hpp>
#include <caf/io/middleman.hpp>

#include "broker/atoms.hh"
#include "broker/backend.hh"
#include "broker/backend_options.hh"
#include "broker/convert.hh"
#include "broker/error.hh"
#include "broker/peer_status.hh"
#include "broker/status.hh"
#include "broker/timeout.hh"
#include "broker/topic.hh"
#include "broker/version.hh"

#include "broker/detail/master_resolver.hh"

using namespace caf;

namespace broker {
namespace detail {

behavior master_resolver(stateful_actor<master_resolver_state>* self) {
  self->set_error_handler([=](error&) {
    if (--self->state.remaining_responses == 0) {
      CAF_LOG_DEBUG("resolver failed to find a master");
      self->send(self->state.who_asked, atom::master::value,
                 make_error(ec::no_such_master, "no master on peers"));
      self->quit();
    }
  });
  return {
    [=](const std::vector<actor>& peers, const std::string& name,
        actor& who_asked) {
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
