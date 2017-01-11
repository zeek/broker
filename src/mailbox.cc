#include "broker/mailbox.hh"

#include "broker/detail/flare_actor.hh"

namespace broker {
namespace detail {

mailbox make_mailbox(detail::flare_actor* actor) {
  return mailbox{actor};
}

} // namespace detail

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
