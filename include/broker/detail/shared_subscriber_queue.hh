#pragma once

#include <caf/flow/subscription.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/make_counted.hpp>
#include <vector>

#include "broker/detail/shared_queue.hh"
#include "broker/message.hh"

namespace broker::detail {

/// A producer-consumer queue for transferring data from the core to a
/// user-defined subscriber. Uses a `flare` to signal available items to the
/// user. Users can read as long as the flare remains active. The core produces
/// items, while the user consumes them.
///
/// The protocol on the flare is as follows:
/// - the flare starts inactive
/// - the flare is active as long as xs_ has at least one item
/// - produce() fires the flare when it adds items to xs_ and xs_ was empty
/// - consume() extinguishes the flare when it removes the last item from xs_
template <class ValueType = data_message>
class shared_subscriber_queue : public shared_queue<ValueType> {
public:
  using value_type = ValueType;

  using super = shared_queue<value_type>;

  using guard_type = typename super::guard_type;

  shared_subscriber_queue(size_t buffer_size) : capacity_(buffer_size) {
    // nop
  }

  /// Must be called before calling `produce` or `consume` on the queue.
  void init(caf::flow::subscription sub) {
    sub_ = std::move(sub);
    sub_.request(capacity_);
  }

  // Called to pull up to `num` items out of the queue. Returns the number of
  // consumed elements.
  template <class F>
  size_t consume(size_t num, size_t* size_before_consume, F fun) {
    size_t n = 0;
    caf::flow::subscription sub_hdl;
    {
      guard_type guard{this->mtx_};
      if (this->xs_.empty()) {
        if (!completed_)
          return 0;
        else
          throw std::out_of_range("shared subscriber queue closed by producer");
      }
      sub_hdl = sub_;
      if (size_before_consume)
        *size_before_consume = this->xs_.size();
      auto n = std::min(num, this->xs_.size());
      BROKER_ASSERT(n > 0);
      if (n == this->xs_.size()) {
        for (auto& x : this->xs_)
          fun(std::move(x));
        this->xs_.clear();
        if (!completed_)
          this->fx_.extinguish_one();
      } else {
        auto b = this->xs_.begin();
        auto e = b + static_cast<ptrdiff_t>(n);
        for (auto i = b; i != e; ++i)
          fun(std::move(*i));
        this->xs_.erase(b, e);
      }
    }
    sub_hdl.request(n);
    return n;
  }

  std::vector<value_type> consume_all() {
    std::vector<value_type> result;
    caf::flow::subscription sub_hdl;
    {
      guard_type guard{this->mtx_};
      if (!this->xs_.empty()) {
        sub_hdl = sub_;
        auto first = std::make_move_iterator(this->xs_.begin());
        auto last = std::make_move_iterator(this->xs_.end());
        result.insert(result.end(), first, last);
        this->xs_.clear();
        if (!completed_)
          this->fx_.extinguish_one();
      } else if (completed_) {
        throw std::out_of_range("shared subscriber queue closed by producer");
      }
    }
    if (sub_hdl) {
      BROKER_ASSERT(!result.empty());
      sub_hdl.request(result.size());
    }
    return result;
  }

  /// Blocks the consumer until the queue becomes non-empty.
  void await_non_empty() {
    this->fx_.await_one();
  }

  /// Inserts the range `[i, e)` into the queue.
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

  /// Called by the producer to signal that no more items get produced.
  /// @warning Calling `produce` after closing the queue causes undefined
  ///          behavior.
  /// @note Calling this function multiple times is ok. Every call past the
  ///       first is simply a no-op.
  void stop_producing() {
    guard_type guard{this->mtx_};
    if (!completed_) {
      completed_ = true;
      if (this->xs_.empty())
        this->fx_.fire();
    }
  }

  /// Called by the consumer to signal to the core that no more items get
  /// consumed.
  /// @warning Calling `consume` after closing the queue causes undefined
  ///          behavior.
  /// @note Calling this function multiple times is ok. Every call past the
  ///       first is simply a no-op.
  void stop_consuming() {
    caf::flow::subscription sub_hdl;
    {
      guard_type guard{this->mtx_};
      if (sub_) {
        sub_hdl = sub_;
        sub_ = nullptr;
      }
    }
    if (sub_hdl)
      sub_hdl.cancel();
  }

  /// @warning call *only* from the context of the producer.
  void filter(const filter_type& new_filter) {
    filter_ = new_filter;
  }

  const auto& filter() const noexcept {
    return filter_;
  }

private:
  /// Configures the amount of items for xs_.
  size_t capacity_;

  /// Signals demand to the core.
  caf::flow::subscription sub_;

  bool completed_ = false;

  filter_type filter_;
};

template <class ValueType = data_message>
using shared_subscriber_queue_ptr
  = caf::intrusive_ptr<shared_subscriber_queue<ValueType>>;

template <class ValueType = data_message>
shared_subscriber_queue_ptr<ValueType> make_shared_subscriber_queue() {
  return caf::make_counted<shared_subscriber_queue<ValueType>>();
}

} // namespace broker::detail
