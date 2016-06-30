#include <poll.h>

#include "broker/logger.hh" // Needs to come before CAF includes

#include <caf/all.hpp>
#include <caf/detail/sync_request_bouncer.hpp>

#include "broker/detail/assert.hh"
#include "broker/detail/flare_actor.hh"

namespace broker {
namespace detail {

flare_actor::flare_actor(caf::actor_config& sys) : blocking_actor{sys} {
  // Ensure that the first enqueue operation returns unblocked_reader.
  mailbox().try_block();
}

void flare_actor::launch(caf::execution_unit*, bool, bool) {
  // Nothing todo for our blocking actor here since we only extract messages
  // via receive calls.
}

void flare_actor::act() {
  // Usually called from launch(). But should never happen in our
  // implementation.
  BROKER_ASSERT(! "act() of flare_actor called");
}

void flare_actor::await_data() {
  BROKER_DEBUG("awaiting data");
  if (has_next_message())
    return;
  pollfd p = {flare_.fd(), POLLIN, {}};
  for (;;) {
    BROKER_DEBUG("polling");
    auto n = ::poll(&p, 1, -1);
    if (n < 0 && errno != EAGAIN)
      std::terminate();
    if (n == 1) {
      BROKER_ASSERT(p.revents & POLLIN);
      BROKER_ASSERT(has_next_message());
      break;
    }
  }
}

bool flare_actor::await_data(timeout_type timeout) {
  BROKER_DEBUG("awaiting data with timeout");
  if (has_next_message())
    return true;
  pollfd p = {flare_.fd(), POLLIN, {}};
  auto delta = timeout - timeout_type::clock::now();
  if (delta.count() <= 0)
    return false;
  auto n = ::poll(&p, 1, delta.count());
  if (n < 0 && errno != EAGAIN)
    std::terminate();
  if (n == 1) {
    BROKER_ASSERT(p.revents & POLLIN);
    BROKER_ASSERT(has_next_message());
    return true;
  }
  return false;
}

void flare_actor::enqueue(caf::mailbox_element_ptr ptr, caf::execution_unit*) {
  auto mid = ptr->mid;
  auto sender = ptr->sender;
  switch (mailbox().enqueue(ptr.release())) {
    case caf::detail::enqueue_result::unblocked_reader: {
      BROKER_DEBUG("firing flare");
      flare_.fire();
      break;
    }
    case caf::detail::enqueue_result::queue_closed:
      if (mid.is_request()) {
        caf::detail::sync_request_bouncer bouncer{caf::exit_reason()};
        bouncer(sender, mid);
      }
      break;
    case caf::detail::enqueue_result::success:
      break;
  }
}

caf::mailbox_element_ptr flare_actor::dequeue() {
  auto msg = next_message();
  if (!has_next_message() && mailbox().try_block()) {
    BROKER_DEBUG("extinguishing flare");
    auto extinguished = flare_.extinguish_one();
    BROKER_ASSERT(extinguished);
  }
  return msg;
}

const char* flare_actor::name() const {
  return "flare_actor";
}

int flare_actor::descriptor() const {
  return flare_.fd();
}

} // namespace detail
} // namespace broker
