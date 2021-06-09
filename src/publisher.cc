#include "broker/logger.hh" // Must come before any CAF include.
#include "broker/publisher.hh"

#include <future>
#include <numeric>

#include <caf/flow/merge.hpp>
#include <caf/flow/observable.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/send.hpp>

#include "broker/data.hh"
#include "broker/detail/flow_controller.hh"
#include "broker/detail/flow_controller_callback.hh"
#include "broker/detail/shared_publisher_queue_adapter.hh"
#include "broker/endpoint.hh"
#include "broker/message.hh"
#include "broker/topic.hh"

namespace broker {

publisher::publisher(queue_ptr q, topic t)
  : queue_(std::move(q)), topic_(std::move(t)) {
  // nop
}

publisher::~publisher() {
  reset();
}

publisher publisher::make(endpoint& ep, topic t) {
  using promise_type = std::promise<queue_ptr>;
  auto qpromise = std::make_shared<promise_type>();
  auto f = qpromise->get_future();
  auto init = detail::make_flow_controller_callback(
    [pptr{std::move(qpromise)}](detail::flow_controller* ctrl) mutable {
      ctrl->connect(std::move(pptr));
    });
  caf::anon_send(ep.core(), std::move(init));
  auto q = f.get();
  return publisher{std::move(q), std::move(t)};
}

size_t publisher::buffered() const {
  return queue_->buffer_size();
}

size_t publisher::capacity() const {
  return queue_->capacity();
}

size_t publisher::free_capacity() const {
  auto x = capacity();
  auto y = buffered();
  return x > y ? x - y : 0;
}

void publisher::drop_all_on_destruction() {
  drop_on_destruction_ = true;
}

void publisher::publish(data x) {
  BROKER_DEBUG("publishing" << std::make_pair(topic_, x));
  queue_->produce(topic_, std::move(x));
}

void publisher::publish(std::vector<data> xs) {
  auto t = static_cast<ptrdiff_t>(queue_->capacity());
  auto i = xs.begin();
  auto e = xs.end();
  while (i != e) {
    auto j = i + std::min(std::distance(i, e), t);
#ifdef DEBUG
    BROKER_DEBUG("publishing batch of size" << (j - i));
    for (auto l = i; l < j; l++) {
      BROKER_DEBUG("publishing" << std::forward_as_tuple(topic_, *l));
    }
#endif
    queue_->produce(topic_, i, j);
    i = j;
  }
}

void publisher::reset() {
  if (queue_) {
    queue_->close(drop_on_destruction_);
    queue_ = nullptr;
  }
}

} // namespace broker
