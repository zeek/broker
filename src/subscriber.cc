#include "broker/subscriber.hh"

#include <chrono>

#include <caf/message.hpp>
#include <caf/send.hpp>
#include <caf/stream_sink.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/upstream.hpp>
#include <caf/upstream_path.hpp>

#include "broker/atoms.hh"
#include "broker/context.hh"
#include "broker/endpoint.hh"

#include "broker/detail/filter_type.hh"

using namespace caf;

namespace broker {

namespace {

using policy_ptr = std::unique_ptr<upstream_policy>;

class subscriber_policy : public upstream_policy {
public:
  // @pre `queue_->mtx` is locked.
  void assign_credit(assignment_vec& xs, long) override {
    // We assume there is only one upstream (the core) and simply ignore the
    // second parameter since we only consider the current state of our buffer.
    BROKER_ASSERT(xs.size() == 1);
    auto size = static_cast<long>(queue_->xs.size());
    BROKER_ASSERT(size <= max_qsize_);
    auto x = max_qsize_ - size;
    queue_->pending = x;
    queue_->cv.notify_one();
    auto assigned = xs.front().first->assigned_credit;
    BROKER_ASSERT(x >= assigned);
    xs.front().second = x - xs.front().first->assigned_credit;
  }

  static policy_ptr make(detail::shared_queue_ptr qptr, long max_qsize) {
    return policy_ptr {new subscriber_policy(std::move(qptr), max_qsize)};
  }

private:
  subscriber_policy(detail::shared_queue_ptr qptr, long max_qsize)
    : queue_(std::move(qptr)),
      max_qsize_(max_qsize) {
    // nop
  }

  detail::shared_queue_ptr queue_;
  long max_qsize_;
};

class subscriber_sink : public extend<stream_handler, subscriber_sink>::
                               with<mixin::has_upstreams> {
public:
  using element_type = std::pair<topic, data>;

  subscriber_sink(event_based_actor* self, detail::shared_queue_ptr qptr,
                  long max_qsize)
    : in_(self, subscriber_policy::make(qptr, max_qsize)),
      queue_(std::move(qptr)) {
    // nop
  }

  expected<long> add_upstream(strong_actor_ptr& hdl, const stream_id& sid,
                              stream_priority prio) override {
    CAF_LOG_TRACE(CAF_ARG(hdl) << CAF_ARG(sid) << CAF_ARG(prio));
    if (hdl)
      return in_.add_path(hdl, sid, prio, 0); // 0 is ignored by the policy.
    return sec::invalid_argument;
  }

  error upstream_batch(strong_actor_ptr& hdl, long xs_size,
                       caf::message& xs) override {
    CAF_LOG_TRACE(CAF_ARG(hdl) << CAF_ARG(xs_size) << CAF_ARG(xs));
    auto path = in_.find(hdl);
    if (path) {
      if (xs_size > path->assigned_credit)
        return sec::invalid_stream_state;
      path->assigned_credit -= xs_size;
      if (!xs.match_elements<std::vector<element_type>>())
        return sec::unexpected_message;
      auto& ys = xs.get_mutable_as<std::vector<element_type>>(0);
      subscriber::guard_type guard{queue_->mtx};
      queue_->xs.insert(queue_->xs.end(), std::make_move_iterator(ys.begin()),
                        std::make_move_iterator(ys.end()));
      in_.assign_credit(0); // 0 is ignored by our policy.
      return caf::none;
    }
    return sec::invalid_upstream;
  }

  bool done() const override {
    return in_.closed();
  }

  void abort(strong_actor_ptr& cause, const error& reason) override {
    CAF_LOG_TRACE(CAF_ARG(cause) << CAF_ARG(reason));
    in_.abort(cause, reason);
  }

  optional<abstract_upstream&> get_upstream() override {
    return in_;
  }

  void last_upstream_closed() {
    CAF_LOG_TRACE("");
  }

private:
  upstream<element_type> in_;
  detail::shared_queue_ptr queue_;
};

behavior subscriber_worker(event_based_actor* self, context* ctx,
                           detail::shared_queue_ptr qptr, std::vector<topic> ts,
                           long max_qsize) {
  self->send(self * ctx->core(), atom::join::value, std::move(ts));
  return {
    [=](const endpoint::stream_type& in) {
      auto mptr = self->current_mailbox_element();
      BROKER_ASSERT(mptr != nullptr);
      auto sptr = make_counted<subscriber_sink>(self, qptr, max_qsize);
      self->streams().emplace(in.id(), std::move(sptr));
    }
  };
}

} // namespace <anonymous>

subscriber::subscriber(context& ctx, std::vector<topic> ts, long max_qsize) {
  queue_ = detail::make_shared_queue();
  worker_ = ctx.system().spawn(subscriber_worker, &ctx, queue_, std::move(ts),
                               max_qsize);
}

subscriber::~subscriber() {
  anon_send_exit(worker_, exit_reason::user_shutdown);
}

subscriber::value_type subscriber::get() {
  auto tmp = get(1);
  BROKER_ASSERT(tmp.size() == 1);
  return std::move(tmp.front());
}

caf::optional<subscriber::value_type> subscriber::get(caf::duration timeout) {
  auto tmp = get(1, timeout);
  if (tmp.size() == 1)
    return std::move(tmp.front());
  return caf::none;
}

std::vector<subscriber::value_type> subscriber::get(size_t num,
                                                    caf::duration timeout) {
  // Reserve space on the heap for the result.
  using std::swap;
  std::vector<value_type> result;
  if (num == 0)
    return result;
  result.reserve(num);
  // Get exclusive access to the queue.
  guard_type guard{queue_->mtx};
  // Initialize required state and formulate predicate for wait_until.
  auto& xs = queue_->xs;
  size_t moved = 0;
  auto xs_filled = [&] { return !xs.empty(); };
  // Get absolute timeout.
  auto t0 = std::chrono::high_resolution_clock::now();
  t0 += timeout;
  // Loop until either a timeout occurs or we are done.
  for (;;) {
    // Wait some time and abort on timeout.
    if (timeout == caf::infinite) {
      while (xs.empty())
        queue_->cv.wait(guard);
    } else {
      if (!queue_->cv.wait_until(guard, t0, xs_filled))
        return result;
    }
    // Move chunk from deque into our buffer.
    auto chunk = std::min(xs.size(), num - moved);
    auto b = xs.begin();
    auto e = b + chunk;
    result.insert(result.end(), std::make_move_iterator(b),
                  std::make_move_iterator(e));
    xs.erase(b, e);
    moved += chunk;
    // Make sure to trigger our worker in case it ran out of credit.
    if (queue_->pending == 0) {
      // Note: it can happen that we trigger the worker multiple times, however
      // there is no harm in doing so (except a few wasted CPU cycles).
      caf::anon_send(worker_, atom::tick::value);
    }
    // Return if we have filled our buffer.
    if (moved == num)
      return result;
  }
}

std::vector<subscriber::value_type> subscriber::poll() {
  using std::swap;
  std::vector<value_type> result;
  // Get exclusive access to the queue.
  guard_type guard{queue_->mtx};
  // Move everything from the queue if not empty.
  auto& xs = queue_->xs;
  if (xs.empty())
    return result;
  auto b = xs.begin();
  auto e = xs.end();
  result.insert(result.end(), std::make_move_iterator(b),
                std::make_move_iterator(e));
  xs.clear();
  // Make sure to trigger our worker in case it ran out of credit.
  if (queue_->pending == 0) {
    // Note: it can happen that we trigger the worker multiple times, however
    // there is no harm in doing so (except a few wasted CPU cycles).
    anon_send(worker_, atom::tick::value);
  }
  return result;
}

size_t subscriber::available() const {
  guard_type guard{queue_->mtx};
  return queue_->xs.size();
}

} // namespace broker
