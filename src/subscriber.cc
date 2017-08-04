#include "broker/logger.hh" // Must come before any CAF include.
#include "broker/subscriber.hh"

#include <chrono>
#include <numeric>

#include <caf/message.hpp>
#include <caf/random_gatherer.hpp>
#include <caf/scheduled_actor.hpp>
#include <caf/send.hpp>
#include <caf/terminal_stream_scatterer.hpp>

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

class subscriber_term : public terminal_stream_scatterer {
public:
  using queue_ptr = detail::shared_subscriber_queue_ptr<>;

  subscriber_term(queue_ptr qptr, long max_qsize)
    : queue_(std::move(qptr)),
      max_qsize_(max_qsize) {
    // nop
  }

  long credit() const override {
    return max_qsize_ - static_cast<long>(queue_->buffer_size());
  }

  queue_ptr& queue() {
    return queue_;
  }

private:
  queue_ptr queue_;
  long max_qsize_;
};

class subscriber_sink : public stream_manager {
public:
  using input_type = std::pair<topic, data>;

  using output_type = void;

  subscriber_sink(scheduled_actor* self, subscriber_worker_state* state,
                  subscriber_term::queue_ptr qptr, long max_qsize)
    : in_(self),
      out_(std::move(qptr), max_qsize),
      state_(state) {
    // nop
  }

  random_gatherer& in() override {
    return in_;
  }

  subscriber_term& out() override {
    return out_;
  }

  bool done() const override {
    return in_.closed();
  }

protected:
  error process_batch(message& msg) override {
    CAF_LOG_TRACE(CAF_ARG(msg));
    if (!msg.match_elements<std::vector<input_type>>())
      return sec::unexpected_message;
    auto& ys = msg.get_mutable_as<std::vector<input_type>>(0);
    auto ys_size = ys.size();
    state_->counter += ys_size;
    out_.queue()->produce(ys_size, std::make_move_iterator(ys.begin()),
                          std::make_move_iterator(ys.end()));
    return caf::none;
  }

  message make_final_result() override {
    return make_message();
  }

private:
  random_gatherer in_;
  subscriber_term out_;
  subscriber_worker_state* state_;
};

behavior subscriber_worker(stateful_actor<subscriber_worker_state>* self,
                           endpoint* ep,
                           detail::shared_subscriber_queue_ptr<> qptr,
                           std::vector<topic> ts, long max_qsize) {
  self->send(self * ep->core(), atom::join::value, std::move(ts));
  self->set_default_handler(skip);
  return {
    [=](const endpoint::stream_type& in) {
      BROKER_ASSERT(qptr != nullptr);
      auto sid = in.id();
      auto nop = [](subscriber_sink&) {};
      self->make_sink_impl<subscriber_sink>(in, nop, &self->state,
                                            qptr, max_qsize);
      self->set_default_handler(print_and_drop);
      self->delayed_send(self, std::chrono::seconds(1), atom::tick::value);
      self->become(
        [=](atom::join a0, atom::update a1, filter_type& f) {
          self->send(ep->core(), a0, a1, sid, std::move(f));
        },
        [=](atom::tick) {
          auto& st = self->state;
          st.tick();
          qptr->rate(st.rate());
          self->delayed_send(self, std::chrono::seconds(1), atom::tick::value);
        }
      );
    }
  };
}

} // namespace <anonymous>

subscriber::subscriber(endpoint& ep, std::vector<topic> ts, long max_qsize) {
  BROKER_INFO("creating subscriber for topic(s)" << ts);
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
  BROKER_INFO("adding topic" << x << "to subscriber");
  auto e = filter_.end();
  auto i = std::find(filter_.begin(), e, x);
  if (i == e) {
    filter_.emplace_back(std::move(x));
    anon_send(worker_, atom::join::value, atom::update::value, filter_);
  }
}

void subscriber::remove_topic(topic x) {
  BROKER_INFO("removing topic" << x << "from subscriber");
  auto e = filter_.end();
  auto i = std::find(filter_.begin(), e, x);
  if (i != filter_.end()) {
    filter_.erase(i);
    anon_send(worker_, atom::join::value, atom::update::value, filter_);
  }
}

} // namespace broker
