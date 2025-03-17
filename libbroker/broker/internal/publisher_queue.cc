#include "broker/internal/publisher_queue.hh"

#include "broker/detail/assert.hh"

namespace broker::internal {

void publisher_queue::on_consumer_ready() {
  // nop
}

void publisher_queue::on_consumer_cancel() {
  guard_type guard{mtx_};
  cancelled_ = true;
  if (demand_ == 0) {
    fx_.fire();
  }
}

void publisher_queue::on_consumer_demand(size_t demand) {
  BROKER_ASSERT(demand > 0);
  guard_type guard{mtx_};
  if (demand_ == 0) {
    demand_ = demand;
    fx_.fire();
  } else {
    demand_ += demand;
  }
  return;
}

void publisher_queue::ref_producer() const noexcept {
  ref();
}

void publisher_queue::deref_producer() const noexcept {
  deref();
}

size_t publisher_queue::demand() const noexcept {
  guard_type guard{mtx_};
  return demand_;
}

void publisher_queue::push(caf::span<const value_type> items) {
  if (items.empty()) {
    return;
  }
  guard_type guard{mtx_};
  if (cancelled_) {
    return;
  }
  while (demand_ == 0) {
    guard.unlock();
    fx_.await_one();
    guard.lock();
    if (cancelled_) {
      return;
    }
  }
  if (items.size() < demand_) {
    demand_ -= items.size();
    guard.unlock();
    buf_->push(items);
    return;
  }
  auto n = demand_;
  demand_ = 0;
  fx_.extinguish();
  guard.unlock();
  buf_->push(items.subspan(0, n));
  push(items.subspan(n));
}

} // namespace broker::internal
