#include "broker/logger.hh" // Must come before any CAF include.
#include "broker/subscriber.hh"

#include <chrono>
#include <cstddef>
#include <future>
#include <numeric>
#include <utility>

#include <caf/scheduled_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>

#include "broker/atoms.hh"
#include "broker/detail/assert.hh"
#include "broker/detail/flow_controller.hh"
#include "broker/detail/flow_controller_callback.hh"
#include "broker/endpoint.hh"
#include "broker/filter_type.hh"

namespace broker {

subscriber::subscriber(queue_ptr queue, filter_type filter, caf::actor core)
  : queue_(std::move(queue)),
    filter_(std::move(filter)),
    core_(std::move(core)) {
  BROKER_INFO("creating subscriber for topic(s)" << filter_);
}

subscriber::~subscriber() {
  reset();
}

subscriber subscriber::make(endpoint& ep, filter_type filter,
                            size_t queue_size) {
  using queue_promise_type = std::promise<queue_ptr>;
  auto queue_promise = std::make_shared<queue_promise_type>();
  auto queue_future = queue_promise->get_future();
  auto init = detail::make_flow_controller_callback(
    [filter_copy{filter},
     qp{std::move(queue_promise)}](detail::flow_controller* ctrl) mutable {
      ctrl->connect(std::move(qp), filter_copy);
    });
  caf::anon_send(ep.core(), std::move(init));
  return subscriber{queue_future.get(), std::move(filter), ep.core()};
}

data_message subscriber::get() {
  auto tmp = get(1);
  BROKER_ASSERT(tmp.size() == 1);
  auto x = std::move(tmp.front());
  BROKER_DEBUG("received" << x);
  return x;
}

std::vector<data_message> subscriber::get(size_t num) {
  return get(num, caf::infinite);
}

std::vector<data_message> subscriber::poll() {
  return queue_->consume_all();
}

void subscriber::add_topic(topic x, bool block) {
  BROKER_INFO("adding topic" << x << "to subscriber");
  auto e = filter_.end();
  if (auto i = std::find(filter_.begin(), e, x); i == e) {
    filter_.emplace_back(std::move(x));
    update_filter(block);
  }
}

void subscriber::remove_topic(topic x, bool block) {
  BROKER_INFO("removing topic" << x << "from subscriber");
  auto e = filter_.end();
  if (auto i = std::find(filter_.begin(), e, x); i != e) {
    filter_.erase(i);
    update_filter(block);
  }
}

void subscriber::reset() {
  if (queue_) {
    queue_->stop_consuming();
    queue_ = nullptr;
    core_ = nullptr;
  }
}

void subscriber::update_filter(bool block) {
  if (!block) {
    auto f = detail::make_flow_controller_callback(
      [qptr{queue_}, fs{filter_}](detail::flow_controller* ctrl) mutable {
        ctrl->update_filter(qptr, fs);
      });
    caf::anon_send(core_, std::move(f));
  } else {
    auto token = std::make_shared<std::promise<void>>();
    auto fut = token->get_future();
    auto f = detail::make_flow_controller_callback(
      [qptr{queue_}, fs{filter_},
       tk{std::move(token)}](detail::flow_controller* ctrl) mutable {
        ctrl->update_filter(qptr, fs);
      });
    caf::anon_send(core_, std::move(f));
    fut.get();
  }
}

} // namespace broker
