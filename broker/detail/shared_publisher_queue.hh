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
template <class ValueType = std::pair<topic, data>>
class shared_publisher_queue : public shared_queue<ValueType> {
public:
  using value_type = ValueType;

  using super = shared_queue<ValueType>;

  using guard_type = typename super::guard_type;

  shared_publisher_queue(size_t buffer_size) : threshold_(buffer_size) {
    // nop
  }

  // Called to pull items out of the queue. Signals demand to the user if less
  // than `num` items can be published from the buffer. When calling consume
  // again after an unsuccessful run, `num` must not be smaller than on the
  // previous call. Otherwise, the demand signaled on the flare runs out of
  // sync.
  template <class F>
  size_t consume(size_t num, F fun) {
    guard_type guard{this->mtx_};
    auto& xs = this->xs_;
    if (xs.empty()) {
      this->pending_ = static_cast<long>(num);
      return false;
    }
    auto n = std::min(num, xs.size());
    auto b = xs.begin();
    auto e = b + static_cast<ptrdiff_t>(n);
    for (auto i = b; i != e; ++i)
      fun(std::move(*i));
    auto xs_old_size = xs.size();
    xs.erase(b, e);
    if (xs.size() < threshold_ && xs_old_size >= threshold_)
      this->fx_.fire();
    if (num - n > 0)
      this->pending_ = static_cast<long>(num - n);
    return n;
  }

  // Returns true if the caller must wake up the consumer.
  template <class Iterator>
  bool produce(const topic& t, Iterator first, Iterator last) {
    BROKER_ASSERT(std::distance(first, last) < threshold_);
    guard_type guard{this->mtx_};
    auto& xs = this->xs_;
    auto xs_old_size = xs.size();
    for (; first != last; ++first)
      xs.emplace_back(t, std::move(*first));
    if (xs.size() >= threshold_) {
      // Block the caller until the consumer catched up.
      guard.unlock();
      this->fx_.await_one();
      this->fx_.extinguish_one();
    }
    return xs_old_size == 0;
  }

  // Returns true if the caller must wake up the consumer.
  bool produce(const topic& t, data&& y) {
    guard_type guard{this->mtx_};
    auto& xs = this->xs_;
    auto xs_old_size = xs.size();
    xs.emplace_back(t, std::move(y));
    if (xs.size() >= threshold_) {
      // Block the caller until the consumer catched up.
      guard.unlock();
      this->fx_.await_one();
      this->fx_.extinguish_one();
    }
    return xs_old_size == 0;
  }

  size_t threshold() const {
    return threshold_;
  }

private:
  // Configures the amound of items for xs_.
  const size_t threshold_;
};

template <class ValueType = std::pair<topic, data>>
using shared_publisher_queue_ptr
  = caf::intrusive_ptr<shared_publisher_queue<ValueType>>;

template <class ValueType = std::pair<topic, data>>
shared_publisher_queue_ptr<ValueType>
make_shared_publisher_queue(size_t buffer_size) {
  return caf::make_counted<shared_publisher_queue<ValueType>>(buffer_size);
}


} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_SHARED_PUBLISHER_QUEUE_HH
