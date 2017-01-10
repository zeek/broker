#include "broker/mailbox.hh"

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

} // namespace broker
