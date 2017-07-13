#include "broker/logger.hh" // Must come before any CAF include.
#include "broker/subscriber.hh"

#include <chrono>

#include <caf/message.hpp>
#include <caf/send.hpp>
#include <caf/stream_sink.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/upstream_path.hpp>

#include "broker/atoms.hh"
#include "broker/endpoint.hh"

#include "broker/detail/filter_type.hh"

using namespace caf;

namespace broker {

namespace {

using detail::filter_type;

/// Defines how many seconds are averaged for the computation of the send rate.
constexpr size_t sample_size = 10;

struct subscriber_worker_state {
  std::vector<size_t> buf;
  size_t counter = 0;

  static const char* name;

  void tick() {
    if (buf.size() < sample_size) {
      buf.push_back(counter);
    } else {
      std::rotate(buf.begin(), buf.begin() + 1, buf.end());
      buf.back() = counter;
    }
    counter = 0;
  }

  size_t rate() {
    return !buf.empty()
           ? std::accumulate(buf.begin(), buf.end(), size_t{0}) / buf.size()
           : 0;
  }
};

const char* subscriber_worker_state::name = "subscriber_worker";

class subscriber_policy : public upstream_policy {
public:
  subscriber_policy(local_actor* selfptr,
                    detail::shared_subscriber_queue_ptr<> qptr, long max_qsize)
    : upstream_policy(selfptr),
      queue_(std::move(qptr)),
      max_qsize_(max_qsize) {
    // nop
  }

  // @pre `queue_->mtx` is locked.
  void fill_assignment_vec(long) override {
    // We assume there is only one upstream (the core) and simply ignore the
    // second parameter since we only consider the current state of our buffer.
    BROKER_ASSERT(assignment_vec_.size() == 1);
    auto size = static_cast<long>(queue_->buffer_size());
    BROKER_ASSERT(size <= max_qsize_);
    auto x = max_qsize_ - size;
    queue_->pending(x);
    auto& avf = assignment_vec_.front();
    auto assigned = avf.first->assigned_credit;
    BROKER_ASSERT(x >= assigned);
    CAF_IGNORE_UNUSED(assigned);
    avf.second = x - avf.first->assigned_credit;
  }

  long max_qsize() const {
    return max_qsize_;
  }

private:
  detail::shared_subscriber_queue_ptr<> queue_;
  long max_qsize_;
};

class subscriber_sink : public extend<stream_handler, subscriber_sink>::
                               with<mixin::has_upstreams> {
public:
  using value_type = std::pair<topic, data>;

  subscriber_sink(event_based_actor* self, subscriber_worker_state* state,
                  detail::shared_subscriber_queue_ptr<> qptr, long max_qsize)
    : in_(self, qptr, max_qsize),
      queue_(std::move(qptr)),
      state_(state) {
    // nop
  }

  expected<long> add_upstream(strong_actor_ptr& hdl, const stream_id& sid,
                              stream_priority prio) override {
    CAF_LOG_TRACE(CAF_ARG(hdl) << CAF_ARG(sid) << CAF_ARG(prio));
    if (hdl)
      return in_.add_path(hdl, sid, prio, in_.max_qsize());
    return sec::invalid_argument;
  }

  error upstream_batch(strong_actor_ptr& hdl, int64_t xs_id, long xs_size,
                       caf::message& xs) override {
    CAF_LOG_TRACE(CAF_ARG(hdl) << CAF_ARG(xs_size) << CAF_ARG(xs));
    auto path = in_.find(hdl);
    if (path) {
      if (xs_size > path->assigned_credit)
        return sec::invalid_stream_state;
      path->last_batch_id = xs_id;
      path->assigned_credit -= xs_size;
      if (!xs.match_elements<std::vector<value_type>>())
        return sec::unexpected_message;
      auto& ys = xs.get_mutable_as<std::vector<value_type>>(0);
      auto ys_size = ys.size();
      CAF_ASSERT(ys_size == xs_size);
      state_->counter += ys_size;
      queue_->produce(ys_size, std::make_move_iterator(ys.begin()),
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

  optional<upstream_policy&> up() override {
    return in_;
  }

  void last_upstream_closed() {
    CAF_LOG_TRACE("");
  }

private:
  subscriber_policy in_;
  detail::shared_subscriber_queue_ptr<> queue_;
  subscriber_worker_state* state_;
};

behavior subscriber_worker(stateful_actor<subscriber_worker_state>* self,
                           endpoint* ep,
                           detail::shared_subscriber_queue_ptr<> qptr,
                           std::vector<topic> ts, long max_qsize) {
  self->send(self * ep->core(), atom::join::value, std::move(ts));
  //self->delayed_send(self, std::chrono::seconds(1), atom::tick::value);
  return {
    [=](const endpoint::stream_type& in) {
      BROKER_ASSERT(qptr != nullptr);
      auto sptr = make_counted<subscriber_sink>(self, &self->state,
                                                qptr, max_qsize);
      self->streams().emplace(in.id(), std::move(sptr));
    },
    [=](atom::join a0, atom::update a1, filter_type& f) {
      self->send(ep->core(), a0, a1, std::move(f));
    },
    [=](atom::tick) {
      auto& st = self->state;
      st.tick();
      qptr->rate(st.rate());
      self->delayed_send(self, std::chrono::seconds(1), atom::tick::value);
    }
  };
}

} // namespace <anonymous>

std::string topics_to_string(std::vector<topic> ts) {
  std::string s;
  for (auto t : ts) {
    if (s.size())
      s += ", ";
    s += t.string();
  }
  return s;
}

subscriber::subscriber(endpoint& ep, std::vector<topic> ts, long max_qsize) {
  CAF_LOG_INFO("creating subscriber for topic(s)" << topics_to_string(ts));

  worker_ = ep.system().spawn(subscriber_worker, &ep, queue_, std::move(ts),
                               max_qsize);
}

subscriber::~subscriber() {
  anon_send_exit(worker_, exit_reason::user_shutdown);
}

size_t subscriber::rate() const {
  return queue_->rate();
}

void subscriber::add_topic(topic x) {
  CAF_LOG_INFO("adding topic" << x << "to subscriber");
  auto e = filter_.end();
  auto i = std::find(filter_.begin(), e, x);
  if (i == e) {
    filter_.emplace_back(std::move(x));
    anon_send(worker_, atom::join::value, atom::update::value, filter_);
  }
}

void subscriber::remove_topic(topic x) {
  CAF_LOG_INFO("removing topic" << x << "from subscriber");
  auto e = filter_.end();
  auto i = std::find(filter_.begin(), e, x);
  if (i != filter_.end()) {
    filter_.erase(i);
    anon_send(worker_, atom::join::value, atom::update::value, filter_);
  }
}

} // namespace broker
