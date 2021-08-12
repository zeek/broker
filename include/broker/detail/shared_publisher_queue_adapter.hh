#pragma once

#include <caf/async/notifiable.hpp>
#include <caf/flow/observable.hpp>
#include <caf/scheduled_actor/flow.hpp>

namespace broker::detail {

template <class T>
class shared_publisher_queue_adapter
  : public caf::flow::buffered_observable_impl<T>,
    public caf::async::notifiable::listener {
public:
  using super = caf::flow::buffered_observable_impl<T>;

  using queue_ptr = detail::shared_publisher_queue_ptr<T>;

  explicit shared_publisher_queue_adapter(caf::flow::coordinator* ctx,
                                          queue_ptr queue)
    : super(ctx, caf::defaults::flow::batch_size), queue_(std::move(queue)) {
    // nop
  }

  void on_event() override {
    this->try_push();
  }

  void on_close() override {
    this->try_push();
    this->shutdown();
  }

  void on_abort(const error& reason) override {
    this->abort(reason);
  }

  void on_request(caf::flow::observer_base* sink, size_t n) override {
    super::on_request(sink, n);
  }

  bool done() const noexcept override {
    return super::done() && queue_->buffer_size() == 0;
  }

protected:
  void pull(size_t n) override {
    BROKER_ASSERT(n > 0);
    queue_->consume(n, [this](auto&& item) {
      this->append_to_buf(std::forward<decltype(item)>(item));
    });
  }

private:
  queue_ptr queue_;
};

} // namespace broker::detail
