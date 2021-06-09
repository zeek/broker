#pragma once

#include <caf/async/notifiable.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/make_counted.hpp>

#include "broker/detail/assert.hh"
#include "broker/detail/shared_queue.hh"
#include "broker/message.hh"

namespace broker::detail {

/// A producer-consumer queue for transferring data from a publisher to the
/// core. Uses a `flare` to signal demand to the user. Users can write as long
/// as the flare remains active. The core consumes items, while the user
/// produces them.
///
/// The protocol on the flare is as follows:
/// - the flare starts active
/// - the flare is active as long as xs_ has below 20 items
/// - consume() fires the flare when it removes items from xs_ and less than 20
///   items remain
/// - produce() extinguishes the flare it adds items to xs_, exceeding 20
template <class ValueType = data_message>
class shared_publisher_queue : public shared_queue<ValueType> {
public:
  using value_type = ValueType;

  using super = shared_queue<ValueType>;

  using guard_type = typename super::guard_type;

  shared_publisher_queue(size_t buffer_size) : capacity_(buffer_size) {
    // The flare is active as long as publishers can write.
    this->fx_.fire();
  }

  /// Must be called before calling `produce` or `consume` on the queue.
  void init(caf::async::notifiable hdl) {
    notify_hdl_ = std::move(hdl);
  }

  /// Pulls up to `num` items out of the queue.
  template <class F>
  size_t consume(size_t num, F fun) {
    guard_type guard{this->mtx_};
    auto& xs = this->xs_;
    auto old_size = xs.size();
    auto n = std::min(num, old_size);
    if (n == 0)
      return 0;
    auto b = xs.begin();
    auto e = b + static_cast<ptrdiff_t>(n);
    for (auto i = b; i != e; ++i)
      fun(std::move(*i));
    xs.erase(b, e);
    auto new_size = xs.size();
    // Fire it if we drop below the capacity again.
    if (new_size < capacity_ && old_size >= capacity_)
      this->fx_.fire();
    return n;
  }

  /// Returns true if the caller must wake up the consumer. This function can
  /// go beyond the capacity of the queue.
  template <class Iterator>
  void produce(const topic& t, Iterator first, Iterator last) {
    guard_type guard{this->mtx_};
    auto& xs = this->xs_;
    if (xs.size() >= capacity_)
      await_consumer(guard);
    auto xs_old_size = xs.size();
    BROKER_ASSERT(xs_old_size < capacity_);
    for (; first != last; ++first)
      xs.emplace_back(t, std::move(*first));
    if (xs.size() >= capacity_) {
      // Extinguish the flare to cause the *next* produce to block.
      this->fx_.extinguish_one();
    }
    if (xs_old_size == 0)
      notify_hdl_.notify_event();
  }

  /// Returns true if the caller must wake up the consumer.
  void produce(const topic& t, data&& y) {
    guard_type guard{this->mtx_};
    auto& xs = this->xs_;
    if (xs.size() >= capacity_)
      await_consumer(guard);
    auto xs_old_size = xs.size();
    BROKER_ASSERT(xs_old_size < capacity_);
    xs.emplace_back(t, std::move(y));
    if (xs.size() >= capacity_) {
      // Extinguish the flare to cause the *next* produce to block.
      this->fx_.extinguish_one();
    }
    if (xs_old_size == 0)
      notify_hdl_.notify_event();
  }

  /// Called by the producer to signal to the core that no more items get
  /// produced.
  /// @warning Calling `produce` after closing the queue causes undefined
  ///          behavior.
  /// @note Calling `close` multiple times is ok. Every call past the first is
  ///       simply a no-op.
  void close(bool drop_remaining = false) {
    guard_type guard{this->mtx_};
    if (notify_hdl_) {
      notify_hdl_.notify_close();
      notify_hdl_ = nullptr;
      if (drop_remaining)
        this->xs_.clear();
    }
  }

  size_t capacity() const {
    return capacity_;
  }

private:
  void await_consumer(guard_type& guard) {
    // Block the caller until the consumer catches up.
    guard.unlock();
    this->fx_.await_one();
    guard.lock();
  }

  /// Configures the amount of items for xs_.
  size_t capacity_;

  caf::async::notifiable notify_hdl_;
};

template <class ValueType = data_message>
using shared_publisher_queue_ptr
  = caf::intrusive_ptr<shared_publisher_queue<ValueType>>;

template <class ValueType = data_message>
shared_publisher_queue_ptr<ValueType>
make_shared_publisher_queue(size_t buffer_size) {
  return caf::make_counted<shared_publisher_queue<ValueType>>(buffer_size);
}

} // namespace broker::detail
