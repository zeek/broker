#include "broker/mailbox.hh"

#include "broker/internal/flare_actor.hh"

namespace broker::internal {

mailbox make_mailbox(internal::flare_actor* actor) {
  return mailbox{actor};
}

} // namespace broker::internal

namespace broker {

detail::native_socket mailbox::descriptor() {
  return actor_->descriptor();
}

bool mailbox::empty() {
  return actor_->mailbox().empty();
}

size_t mailbox::size() {
  return actor_->mailbox().size();
}

mailbox::mailbox(internal::flare_actor* actor) : actor_{actor} {
  // nop
}

} // namespace broker
