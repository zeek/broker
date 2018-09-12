#include "broker/status_subscriber.hh"

#include <limits>

#include <caf/send.hpp>
#include <caf/event_based_actor.hpp>

#include "broker/atoms.hh"
#include "broker/endpoint.hh"

using namespace caf;

namespace broker {

namespace {

behavior status_subscriber_worker(event_based_actor* self,
                                 bool receive_statuses,
                                 status_subscriber::queue_ptr qptr) {
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

status_subscriber::status_subscriber(endpoint& ep, bool receive_statuses) {
  worker_ = ep.system().spawn(status_subscriber_worker, receive_statuses,
                              queue_);
}

status_subscriber::~status_subscriber() {
  anon_send_exit(worker_, exit_reason::user_shutdown);
}

} // namespace broker
