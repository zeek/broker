#include <caf/event_based_actor.hpp>
#include <caf/send.hpp>

#include "broker/nonblocking_endpoint.hh"

#include "broker/detail/core_actor.hh"

namespace broker {

nonblocking_endpoint::nonblocking_endpoint(caf::actor_system& sys,
                                           caf::behavior bhvr) {
  auto subscriber = [=](caf::event_based_actor* self) {
    self->set_default_handler(caf::drop); // avoids unintended leaks
    return bhvr;
  };
  subscriber_ = sys.spawn(subscriber);
  init_core(sys.spawn(detail::core_actor, subscriber_));
}

} // namespace broker
