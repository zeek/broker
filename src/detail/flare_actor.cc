#include "broker/detail/flare_actor.hh"

#include <caf/detail/enqueue_result.hpp>
#include <caf/detail/sync_request_bouncer.hpp>
#include <caf/execution_unit.hpp>
#include <caf/mailbox_element.hpp>

#include "broker/logger.hh"

namespace broker {
namespace detail {

flare_actor::flare_actor(caf::actor_config& sys)
    : blocking_actor{sys},
      flare_count_{0} {
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
  BROKER_DEBUG("awaiting data");
  std::unique_lock<std::mutex> lock{flare_mtx_};
  if (flare_count_ > 0 )
    return;
  lock.unlock();
  flare_.await_one();
}

bool flare_actor::await_data(timeout_type timeout) {
  BROKER_DEBUG("awaiting data with timeout");
  std::unique_lock<std::mutex> lock{flare_mtx_};
  if (flare_count_ > 0)
    return true;
  lock.unlock();
  auto res = flare_.await_one(timeout);
  return res;
}

void flare_actor::enqueue(caf::mailbox_element_ptr ptr, caf::execution_unit*) {
  auto mid = ptr->mid;
  auto sender = ptr->sender;
  std::unique_lock<std::mutex> lock{flare_mtx_};
  switch (mailbox().enqueue(ptr.release())) {
    case caf::detail::enqueue_result::unblocked_reader: {
      BROKER_DEBUG("firing flare");
      flare_.fire();
      ++flare_count_;
      break;
    }
    case caf::detail::enqueue_result::queue_closed:
      if (mid.is_request()) {
        caf::detail::sync_request_bouncer bouncer{caf::exit_reason{}};
        bouncer(sender, mid);
      }
      break;
    case caf::detail::enqueue_result::success: {
      flare_.fire();
      ++flare_count_;
      break;
    }
  }
}

caf::mailbox_element_ptr flare_actor::dequeue() {
  std::unique_lock<std::mutex> lock{flare_mtx_};
  auto rval = blocking_actor::dequeue();

  if (rval)
    this->extinguish_one();

  return rval;
}

const char* flare_actor::name() const {
  return "flare_actor";
}

void flare_actor::extinguish_one() {
  std::unique_lock<std::mutex> lock{flare_mtx_};
  auto extinguished = flare_.extinguish_one();
  CAF_ASSERT(extinguished);
  --flare_count_;
}

} // namespace detail
} // namespace broker
