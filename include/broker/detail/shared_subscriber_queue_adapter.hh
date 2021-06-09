#pragma once

#include <future>

#include <caf/async/notifiable.hpp>
#include <caf/flow/observer.hpp>
#include <caf/scheduled_actor/flow.hpp>

#include "broker/detail/prefix_matcher.hh"
#include "broker/filter_type.hh"

namespace broker::detail {

template <class T>
class shared_subscriber_queue_adapter : public caf::flow::observer<T>::impl {
public:
  using queue_ptr = shared_subscriber_queue_ptr<T>;

  using promise_ptr = std::shared_ptr<std::promise<queue_ptr>>;

  shared_subscriber_queue_adapter(caf::scheduled_actor* self, queue_ptr queue,
                                  promise_ptr promise)
    : queue_(std::move(queue)), promise_(std::move(promise)), self_(self) {
    // nop
  }

  void dispose() override {
    if (queue_) {
      queue_->stop_producing();
      if (sub_) {
        sub_.cancel();
        sub_ = nullptr;
      }
    }
  }

  bool disposed() const noexcept override {
    return queue_ != nullptr;
  }

  void on_complete() override {
    sub_ = nullptr;
    dispose();
  }

  void on_error(const error& what) override {
    sub_ = nullptr;
    dispose();
  }

  void on_attach(caf::flow::subscription sub) override {
    if (promise_) {
      sub_ = sub;
      queue_->init(self_->to_async_subscription(std::move(sub)));
      promise_->set_value(queue_);
      promise_ = nullptr;
    } else {
      sub.cancel();
    }
  }

  void on_next(caf::span<const T> items) override {
    prefix_matcher predicate;
    buf_.clear();
    for (const auto& item : items)
      if (predicate(queue_->filter(), get_topic(item)))
        buf_.emplace_back(item);
    if (items.size() > buf_.size())
      sub_.request(items.size() - buf_.size());
    if (!buf_.empty()) {
      queue_->produce(buf_.size(), buf_.begin(), buf_.end());
      buf_.clear();
    }
  }

private:
  queue_ptr queue_;
  promise_ptr promise_;
  std::vector<T> buf_;
  caf::flow::subscription sub_;
  caf::scheduled_actor* self_;
};

template <class T>
using shared_subscriber_queue_adapter_ptr
  = caf::intrusive_ptr<shared_subscriber_queue_adapter<T>>;

} // namespace broker::detail
