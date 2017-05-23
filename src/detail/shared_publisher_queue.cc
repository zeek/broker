#include "broker/detail/shared_publisher_queue.hh"

namespace broker {
namespace detail {

shared_publisher_queue::shared_publisher_queue(size_t buffer_size)
  : threshold_(buffer_size) {
  // nop
}

bool shared_publisher_queue::produce(const topic& t, std::vector<data>&& ys) {
  guard_type guard{mtx_};
  auto xs_old_size = xs_.size();
  for (auto& y : ys)
    xs_.emplace_back(t, std::move(y));
  if (xs_old_size < threshold_ && xs_.size() >= threshold_)
    fx_.extinguish_one();
  return xs_old_size == 0;
}

bool shared_publisher_queue::produce(const topic& t, data&& y) {
  guard_type guard{mtx_};
  auto xs_old_size = xs_.size();
  xs_.emplace_back(t, std::move(y));
  if (xs_old_size + 1 == threshold_)
    fx_.extinguish_one();
  return xs_old_size == 0;
}

shared_publisher_queue_ptr make_shared_publisher_queue(size_t buffer_size) {
  return caf::make_counted<shared_publisher_queue>(buffer_size);
}

} // namespace detail
} // namespace broker
