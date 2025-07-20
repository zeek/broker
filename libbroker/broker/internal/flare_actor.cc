#include "broker/internal/flare_actor.hh"

#include <caf/detail/sync_request_bouncer.hpp>
#include <caf/execution_unit.hpp>
#include <caf/intrusive/inbox_result.hpp>
#include <caf/mailbox_element.hpp>

#include "broker/detail/assert.hh"

namespace broker::internal {

flare_actor::flare_actor(caf::actor_config& sys) : blocking_actor{sys} {}

void flare_actor::launch(caf::execution_unit*, bool, bool) {
  // Nothing todo here since we only extract messages via receive() calls.
}

void flare_actor::act() {
  // Usually called from launch(). But should never happen in our
  // implementation.
  CAF_ASSERT(!"act() of flare_actor called");
}

void flare_actor::await_data() {
  std::unique_lock<std::mutex> lock{flare_mtx_};
  if (flare_count_ > 0)
    return;
  lock.unlock();
  flare_.await_one();
}

bool flare_actor::await_data(timeout_type timeout) {
  std::unique_lock<std::mutex> lock{flare_mtx_};
  if (flare_count_ > 0)
    return true;
  lock.unlock();
  auto res = flare_.await_one(timeout);
  return res;
}

bool flare_actor::enqueue(caf::mailbox_element_ptr ptr, caf::execution_unit*) {
  auto mid = ptr->mid;
  auto sender = ptr->sender;
  std::unique_lock<std::mutex> lock{flare_mtx_};
  switch (mailbox().enqueue(ptr.release())) {
    case caf::intrusive::inbox_result::unblocked_reader:
    case caf::intrusive::inbox_result::success:
      flare_.fire();
      ++flare_count_;
      return true;
    default: // caf::detail::enqueue_result::queue_closed
      if (mid.is_request()) {
        caf::detail::sync_request_bouncer bouncer{caf::exit_reason{}};
        bouncer(sender, mid);
      }
      return false;
  }
}

caf::mailbox_element_ptr flare_actor::dequeue() {
  std::unique_lock<std::mutex> lock{flare_mtx_};
  auto rval = blocking_actor::dequeue();

  if (rval) {
    [[maybe_unused]] auto extinguished = flare_.extinguish_one();
    BROKER_ASSERT(extinguished);
    --flare_count_;
  }

  return rval;
}

const char* flare_actor::name() const {
  return "flare_actor";
}

void flare_actor::extinguish_one() {
  std::unique_lock<std::mutex> lock{flare_mtx_};
  [[maybe_unused]] auto extinguished = flare_.extinguish_one();
  BROKER_ASSERT(extinguished);
  --flare_count_;
}

} // namespace broker::internal
