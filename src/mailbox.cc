#include "broker/mailbox.hh"

#include "broker/detail/flare_actor.hh"

namespace broker {
namespace detail {

mailbox make_mailbox(detail::flare_actor* actor) {
  return mailbox{actor};
}

} // namespace detail

caf::io::network::native_socket mailbox::descriptor() {
  return actor_->descriptor();
}

bool mailbox::empty() {
  // Make sure to not access fifo_inbox::empty when blocked.
  auto& mbox = actor_->mailbox();
  return mbox.blocked() ? mbox.queue().empty() : mbox.empty();
}

size_t mailbox::size() {
  // Make sure to not access fifo_inbox::size when blocked.
  auto& mbox = actor_->mailbox();
  return mbox.blocked() ? mbox.queue().total_task_size() : mbox.size();
}

size_t mailbox::count(size_t) {
  return size();
}

mailbox::mailbox(detail::flare_actor* actor) : actor_{actor} {
}

} // namespace broker
