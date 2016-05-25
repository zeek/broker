#include <poll.h>

#include <caf/detail/sync_request_bouncer.hpp>

#include "broker/detail/assert.hh"
#include "broker/detail/scoped_flare_actor.hh"

namespace broker {
namespace detail {

flare_actor::flare_actor(caf::actor_config& sys)
  : super{sys.add_flag(caf::local_actor::is_blocking_flag)} {
  set_default_handler(caf::skip);
}

void flare_actor::initialize() {
  // nop
}

void flare_actor::act() {
  CAF_LOG_ERROR("act() of flare_actor called");
}

void flare_actor::enqueue(caf::mailbox_element_ptr ptr, caf::execution_unit*) {
  auto mid = ptr->mid;
  auto sender = ptr->sender;
  switch (mailbox().enqueue(ptr.release())) {
    case caf::detail::enqueue_result::unblocked_reader: {
      flare_.fire();
      break;
    }
    case caf::detail::enqueue_result::queue_closed: {
      if (mid.is_request()) {
        caf::detail::sync_request_bouncer srb{caf::exit_reason()};
        srb(sender, mid);
      }
      break;
    }
    case caf::detail::enqueue_result::success:
      flare_.fire();
      break;
  }
}

// Implementation copied from caf::blocking_actor::dequeue.
void flare_actor::dequeue(caf::behavior& bhvr, caf::message_id mid) {
  if (invoke_from_cache(bhvr, mid))
    return;
  auto timeout_id = request_timeout(bhvr.timeout());
  for (;;) {
    await_flare();
    auto msg = next_message();
    switch (invoke_message(msg, bhvr, mid)) {
      default:
        break;
      case caf::im_success: {
        reset_timeout(timeout_id);
        if (mailbox().try_block()) {
          auto extinguished = flare_.extinguish_one();
          BROKER_ASSERT(extinguished);
        }
        return;
      }
      case caf::im_skipped:
        if (msg)
          push_to_cache(std::move(msg));
        break;
    }
  }
}

caf::message flare_actor::dequeue() {
  await_flare();
  auto msg = next_message()->msg;
  BROKER_ASSERT(! msg.empty());
  auto extinguished = flare_.extinguish_one();
  BROKER_ASSERT(extinguished);
  return msg;
}

void flare_actor::await_flare(std::chrono::milliseconds timeout) {
  pollfd p = {flare_.fd(), POLLIN};
  for (;;) {
    auto n = ::poll(&p, 1, timeout.count());
    if (n < 0 && errno != EAGAIN)
      std::terminate();
    if (n == 1) {
      BROKER_ASSERT(p.revents & POLLIN);
      return;
    }
  }
}

const char* flare_actor::name() const {
  return "flare_actor";
}

int flare_actor::descriptor() const {
  return flare_.fd();
}

int mailbox::descriptor() {
  return actor_->descriptor();
}

bool mailbox::empty() {
  return actor_->mailbox().empty();
}

size_t mailbox::count(size_t max) {
  return actor_->mailbox().count(max);
}

mailbox::mailbox(flare_actor* actor) : actor_{actor} {
}

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
