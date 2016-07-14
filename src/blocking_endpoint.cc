#include <caf/all.hpp>

#include "broker/blocking_endpoint.hh"

#include "broker/detail/assert.hh"
#include "broker/detail/core_actor.hh"
#include "broker/detail/die.hh"
#include "broker/detail/flare_actor.hh"

namespace broker {

int mailbox::descriptor() {
  return actor_->descriptor();
}

bool mailbox::empty() {
  return actor_->mailbox().empty();
}

size_t mailbox::count(size_t max) {
  return actor_->mailbox().count(max);
}

mailbox::mailbox(detail::flare_actor* actor) : actor_{actor} {
}

message blocking_endpoint::receive() {
  auto subscriber = caf::actor_cast<caf::blocking_actor*>(subscriber_);
  subscriber->await_data();
  auto msg = subscriber->dequeue()->move_content_to_message();
  BROKER_ASSERT(!msg.empty());
  return msg;
};

mailbox blocking_endpoint::mailbox() {
  auto subscriber = caf::actor_cast<detail::flare_actor*>(subscriber_);
  return broker::mailbox{subscriber};
}

blocking_endpoint::blocking_endpoint(caf::actor_system& sys) {
  subscriber_ = sys.spawn<detail::flare_actor>();
  init_core(sys.spawn(detail::core_actor, subscriber_));
}

} // namespace broker
