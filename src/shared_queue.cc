#include "broker/subscriber.hh"

namespace broker {
namespace detail {

shared_queue::shared_queue() : pending_(0) {
  // nop
}

size_t shared_queue::buffer_size() const {
  guard_type guard{mtx_};
  return xs_.size();
}

bool shared_queue::wait_on_flare(caf::duration timeout) {
  if (!timeout.valid()) {
    fx_.await_one();
    return true;
  }
  auto abs_timeout = std::chrono::high_resolution_clock::now();
  abs_timeout += timeout;
  return fx_.await_one(abs_timeout);
}

} // namespace detail
} // namespace broker
