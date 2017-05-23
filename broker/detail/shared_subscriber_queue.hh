#ifndef BROKER_DETAIL_SHARED_SUBSCRIBER_QUEUE_HH
#define BROKER_DETAIL_SHARED_SUBSCRIBER_QUEUE_HH

#include <caf/duration.hpp>

#include "broker/detail/shared_queue.hh"

namespace broker {
namespace detail {

/// Synchronizes a publisher with a background worker. Uses the `pending` flag
/// and the `flare` to signalize demand to the user. Users can write as long as
/// the flare remains active. The user consumes items, while the worker 
/// produces them.
///
/// The protocol on the flare is as follows:
/// - the flare starts inactive
/// - the flare is active as long as xs_ has more than one item
/// - produce() fires the flare when it adds items to xs_ and xs_ was empty
/// - consume() extinguishes the flare when it removes the last item from xs_
class shared_subscriber_queue : public shared_queue {
public:
  using element_type = std::pair<topic, data>;

  shared_subscriber_queue() = default;

  ~shared_subscriber_queue();

  // Called to pull up to `num` items out of the queue. Returns the number of
  // consumed elements.
  template <class F>
  size_t consume(size_t num, F fun) {
    guard_type guard{mtx_};
    if (xs_.empty())
      return 0;
    auto n = std::min(num, xs_.size());
    if (n == xs_.size()) {
      for (auto& x : xs_)
        fun(std::move(x));
      xs_.clear();
      fx_.extinguish_one();
    } else {
      auto b = xs_.begin();
      auto e = b + static_cast<ptrdiff_t>(n);
      for (auto i = b; i != e; ++i)
        fun(std::move(*i));
      xs_.erase(b, e);
    }
    return n;
  }

  // Inserts the range `[i, e)` into the queue.
  template <class Iter>
  void produce(size_t num, Iter i, Iter e) {
    CAF_ASSERT(num == std::distance(i, e));
    guard_type guard{mtx_};
    if (xs_.empty())
      fx_.fire();
    xs_.insert(xs_.end(), i, e);
  }
};

using shared_subscriber_queue_ptr = caf::intrusive_ptr<shared_subscriber_queue>;

shared_subscriber_queue_ptr make_shared_subscriber_queue();

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_SHARED_SUBSCRIBER_QUEUE_HH
