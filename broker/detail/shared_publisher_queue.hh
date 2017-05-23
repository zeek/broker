#ifndef BROKER_DETAIL_SHARED_PUBLISHER_QUEUE_HH
#define BROKER_DETAIL_SHARED_PUBLISHER_QUEUE_HH

#include <caf/duration.hpp>

#include "broker/detail/shared_queue.hh"

namespace broker {
namespace detail {

/// Synchronizes a publisher with a background worker. Uses the `pending` flag
/// and the `flare` to signalize demand to the user. Users can write as long as
/// the flare remains active. The worker consumes items, while the user
/// produces them.
///
/// The protocol on the flare is as follows:
/// - the flare starts active
/// - the flare is active as long as xs_ has below 20 items
/// - consume() fires the flare when it removes items from xs_ and less than 20
///   items remain
/// - produce() extinguishes the flare it adds items to xs_, exceeding 20
class shared_publisher_queue : public shared_queue {
public:
  using element_type = std::pair<topic, data>;

  shared_publisher_queue(size_t buffer_size);

  // Called to pull items out of the queue. Signals demand to the user if less
  // than `num` items can be published from the buffer. When calling consume
  // again after an unsuccessful run, `num` must not be smaller than on the
  // previous call. Otherwise, the demand signaled on the flare runs out of
  // sync.
  template <class F>
  size_t consume(size_t num, F fun) {
    guard_type guard{mtx_};
    if (xs_.empty()) {
      pending_ = static_cast<long>(num);
      return false;
    }
    auto n = std::min(num, xs_.size());
    auto b = xs_.begin();
    auto e = b + static_cast<ptrdiff_t>(n);
    for (auto i = b; i != e; ++i)
      fun(std::move(*i));
    auto xs_old_size = xs_.size();
    xs_.erase(b, e);
    if (xs_.size() < threshold_ && xs_old_size > threshold_)
      fx_.fire();
    if (num - n > 0)
      pending_ = static_cast<long>(num - n);
    return n;
  }

  // Returns true if the caller must wake up the consumer.
  bool produce(const topic& t, std::vector<data>&& ys);

  // Returns true if the caller must wake up the consumer.
  bool produce(const topic& t, data&& ys);

private:
  // Configures the amound of items for xs_.
  const size_t threshold_;
};

using shared_publisher_queue_ptr = caf::intrusive_ptr<shared_publisher_queue>;

shared_publisher_queue_ptr make_shared_publisher_queue(size_t buffer_size);

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_SHARED_PUBLISHER_QUEUE_HH
