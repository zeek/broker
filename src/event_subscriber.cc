#include "broker/logger.hh" // Must come before any CAF include.
#include "broker/event_subscriber.hh"

#include <chrono>

#include <caf/message.hpp>
#include <caf/send.hpp>
#include <caf/event_based_actor.hpp>

#include "broker/atoms.hh"
#include "broker/endpoint.hh"

#include "broker/detail/filter_type.hh"

using namespace caf;

namespace broker {

namespace {

behavior event_subscriber_worker(event_based_actor* self,
                                 bool receive_statuses,
                                 event_subscriber::queue_ptr qptr) {
  self->join(self->system().groups().get_local("broker/errors"));
  if (receive_statuses)
    self->join(self->system().groups().get_local("broker/statuses"));
  return {
    [=](atom::local, error& x) {
      qptr->produce(std::move(x));
    },
    [=](atom::local, status& x) {
      qptr->produce(std::move(x));
    }
  };
}

} // namespace <anonymous>

event_subscriber::event_subscriber(endpoint& ep, bool receive_statuses) {
  worker_ = ep.system().spawn(event_subscriber_worker, receive_statuses,
                              queue_);
}

event_subscriber::~event_subscriber() {
  anon_send_exit(worker_, exit_reason::user_shutdown);
}

} // namespace broker
