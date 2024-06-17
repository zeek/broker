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
  // Make sure to not access fifo_inbox::empty when blocked.
  return actor_->mailbox().empty();
}

size_t mailbox::size() {
  return actor_->mailbox().size();
  // // Make sure to not access fifo_inbox::size when blocked.
  // auto& mbox = actor_->mailbox();
  // return mbox.blocked() ? mbox.queue().total_task_size() : mbox.size();
}

size_t mailbox::count(size_t) {
  return size();
}

mailbox::mailbox(internal::flare_actor* actor) : actor_{actor} {
  // nop
}

} // namespace broker
