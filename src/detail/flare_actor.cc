#include "broker/logger.hh" // Needs to come before CAF includes

#include <caf/execution_unit.hpp>
#include <caf/mailbox_element.hpp>
#include <caf/detail/enqueue_result.hpp>
#include <caf/detail/sync_request_bouncer.hpp>

#include "broker/detail/flare_actor.hh"

namespace broker {
namespace detail {

flare_actor::flare_actor(caf::actor_config& sys)
    : blocking_actor{sys},
      await_flare_{true} {
  // Ensure that the first enqueue operation returns unblocked_reader.
  mailbox().try_block();
}

void flare_actor::launch(caf::execution_unit*, bool, bool) {
  // Nothing todo here since we only extract messages via receive() calls.
}

void flare_actor::act() {
  // Usually called from launch(). But should never happen in our
  // implementation.
  CAF_ASSERT(! "act() of flare_actor called");
}

void flare_actor::await_data() {
  CAF_LOG_DEBUG("awaiting data");
  if (! await_flare_)
    return;
  await_flare_ = false;
  flare_.await_one();
}

bool flare_actor::await_data(timeout_type timeout) {
  CAF_LOG_DEBUG("awaiting data with timeout");
  if (! await_flare_)
    return true;
  auto res = flare_.await_one(timeout);
  if (res)
    await_flare_ = false;
  return res;
}

void flare_actor::enqueue(caf::mailbox_element_ptr ptr, caf::execution_unit*) {
  auto mid = ptr->mid;
  auto sender = ptr->sender;
  switch (mailbox().enqueue(ptr.release())) {
    case caf::detail::enqueue_result::unblocked_reader: {
      CAF_LOG_DEBUG("firing flare");
      flare_.fire();
      break;
    }
    case caf::detail::enqueue_result::queue_closed:
      if (mid.is_request()) {
        caf::detail::sync_request_bouncer bouncer{caf::exit_reason{}};
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
    auto extinguished = flare_.extinguish_one();
    CAF_ASSERT(extinguished);
    await_flare_ = true;
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
