#include "broker/detail/flare_actor.hh"

namespace broker {
namespace detail {

scoped_flare_actor::scoped_flare_actor(caf::actor_system& sys)
  : context_{&sys} {
  caf::actor_config cfg{&context_};
  self_ = caf::make_actor<flare_actor, caf::strong_actor_ptr>(
    sys.next_actor_id(), sys.node(), &sys, cfg);
  ptr()->is_registered(true);
}

scoped_flare_actor::~scoped_flare_actor() {
  if (! self_)
    return;
  if (! ptr()->is_terminated())
    ptr()->cleanup(caf::exit_reason::normal, &context_);
}

flare_actor* scoped_flare_actor::operator->() const {
  return ptr();
}

flare_actor& scoped_flare_actor::operator*() const {
  return *ptr();
}

caf::actor_addr scoped_flare_actor::address() const {
  return ptr()->address();
}

flare_actor* scoped_flare_actor::ptr() const {
  auto a = caf::actor_cast<caf::abstract_actor*>(self_);
  return static_cast<flare_actor*>(a);
}

caf::message scoped_flare_actor::dequeue() {
  return ptr()->dequeue();
}

void scoped_flare_actor::dequeue(caf::behavior& bhvr) {
  return ptr()->dequeue(bhvr, caf::invalid_message_id);
}

detail::mailbox scoped_flare_actor::mailbox() {
  return detail::mailbox{ptr()};
}

caf::actor_control_block* scoped_flare_actor::get() const {
  return self_.get();
}

} // namespace detail
} // namespace broker

