#ifndef BROKER_DETAIL_SHARED_SUBSCRIBER_QUEUE_HH
#define BROKER_DETAIL_SHARED_SUBSCRIBER_QUEUE_HH

#include <caf/intrusive_ptr.hpp>
#include <caf/make_counted.hpp>

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
template <class ValueType = std::pair<topic, data>>
class shared_subscriber_queue : public shared_queue<ValueType> {
public:
  using value_type = ValueType;

  using super = shared_queue<ValueType>;

  using guard_type = typename super::guard_type;

  shared_subscriber_queue() = default;

  // Called to pull up to `num` items out of the queue. Returns the number of
  // consumed elements.
  template <class F>
  size_t consume(size_t num, size_t* size_before_consume, F fun) {
    guard_type guard{this->mtx_};
    if (this->xs_.empty())
      return 0;
    if (size_before_consume)
      *size_before_consume = this->xs_.size();
    auto n = std::min(num, this->xs_.size());
    if (n == this->xs_.size()) {
      for (auto& x : this->xs_)
        fun(std::move(x));
      this->xs_.clear();
      this->fx_.extinguish_one();
    } else {
      auto b = this->xs_.begin();
      auto e = b + static_cast<ptrdiff_t>(n);
      for (auto i = b; i != e; ++i)
        fun(std::move(*i));
      this->xs_.erase(b, e);
    }
    return n;
  }

  // Inserts the range `[i, e)` into the queue.
  template <class Iter>
  void produce(size_t num, Iter i, Iter e) {
    CAF_IGNORE_UNUSED(num);
    CAF_ASSERT(num == std::distance(i, e));
    guard_type guard{this->mtx_};
    if (this->xs_.empty())
      this->fx_.fire();
    this->xs_.insert(this->xs_.end(), i, e);
  }

  // Inserts `x` into the queue.
  void produce(ValueType x) {
    guard_type guard{this->mtx_};
    if (this->xs_.empty())
      this->fx_.fire();
    this->xs_.emplace_back(std::move(x));
  }
};

template <class ValueType = std::pair<topic, data>>
using shared_subscriber_queue_ptr
  = caf::intrusive_ptr<shared_subscriber_queue<ValueType>>;

template <class ValueType = std::pair<topic, data>>
shared_subscriber_queue_ptr<ValueType> make_shared_subscriber_queue() {
  return caf::make_counted<shared_subscriber_queue<ValueType>>();
}

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_SHARED_SUBSCRIBER_QUEUE_HH
