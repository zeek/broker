#include "broker/publisher.hh"

#include <caf/event_based_actor.hpp>
#include <caf/send.hpp>
#include <caf/stream_source.hpp>

#include "broker/context.hh"
#include "broker/data.hh"
#include "broker/topic.hh"

using namespace caf;

namespace broker {

namespace {

behavior publisher_worker(event_based_actor* self, context* ctx,
                          detail::shared_queue_ptr qptr) {
  auto handler = self->new_stream(
    ctx->core(),
    [](unit_t&) {
      // nop
    },
    [=](unit_t&, downstream<detail::element_type>& out, size_t num) {
      publisher::guard_type guard{qptr->mtx};
      auto& xs = qptr->xs;
      if (xs.empty()) {
        qptr->pending = static_cast<long>(num);
        qptr->cv.notify_one();
      } else {
        auto n = std::min(num, xs.size());
        for (size_t i = 0u; i < n; ++i)
          out.push(xs[i]);
        xs.erase(xs.begin(), xs.begin() + static_cast<ptrdiff_t>(n));
        if (num - n > 0) {
          qptr->pending = static_cast<long>(num - n);
        }
      }
    },
    [](const unit_t&) {
      return false;
    },
    [](expected<void>) {
      // nop
    }
  ).ptr();
  return {
    [=](atom::resume) {
      static_cast<stream_source*>(handler.get())->generate();
      handler->push();
    },
    [=](atom::tick) {
      // TODO: compute rate
    }
  };
}


} // namespace <anonymous>

publisher::publisher(context& ctx, topic t)
  : queue_(detail::make_shared_queue()),
    worker_(ctx.system().spawn(publisher_worker, &ctx, queue_)),
    topic_(std::move(t)) {
  // nop
}

publisher::~publisher() {
  anon_send_exit(worker_, exit_reason::user_shutdown);
}

size_t publisher::demand() const {
  return queue_->pending.load();
}

size_t publisher::buffered() const {
  guard_type guard{queue_->mtx};
  return queue_->xs.size();

}

size_t publisher::send_rate() const {
  return queue_->rate.load();
}

void publisher::publish(data x) {
  bool trigger_resume = false;
  {
    guard_type guard{queue_->mtx};
    if (queue_->xs.empty())
      trigger_resume = true;
    queue_->xs.emplace_back(topic_, std::move(x));
  }
  if (trigger_resume)
    anon_send(worker_, atom::resume::value);
}

void publisher::publish(std::vector<data> xs) {
  bool trigger_resume = false;
  {
    guard_type guard{queue_->mtx};
    if (queue_->xs.empty())
      trigger_resume = true;
    for (auto& x : xs)
      queue_->xs.emplace_back(topic_, std::move(x));
  }
  if (trigger_resume)
    anon_send(worker_, atom::resume::value);
}

bool publisher::wait_for_demand(size_t min_demand, duration timeout) {
  auto x = demand();
  if (x >= min_demand)
    return true;
  // Get exclusive access to the queue.
  guard_type guard{queue_->mtx};
  if (timeout.valid()) {
    auto abs_timeout = std::chrono::high_resolution_clock::now();
    abs_timeout += timeout;
    auto demand_reached = [=] {
      return demand() >= min_demand;
    };
    if (!queue_->cv.wait_until(guard, abs_timeout, demand_reached))
      return false;
  } else {
    do {
      queue_->cv.wait(guard);
      x = demand();
    } while (x < min_demand);
  }
  return true;
}

} // namespace broker
