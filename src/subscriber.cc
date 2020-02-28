#include "broker/logger.hh" // Must come before any CAF include.
#include "broker/subscriber.hh"

#include <cstddef>
#include <utility>
#include <chrono>
#include <numeric>

#include <caf/scheduled_actor.hpp>
#include <caf/send.hpp>

#include "broker/atoms.hh"
#include "broker/endpoint.hh"
#include "broker/filter_type.hh"
#include "broker/logger.hh"

#include "broker/detail/assert.hh"

using namespace caf;

namespace broker {

namespace {

/// Defines how many seconds are averaged for the computation of the send rate.
constexpr size_t sample_size = 10;

struct subscriber_worker_state {
  std::vector<size_t> buf;
  size_t counter = 0;

  bool calculate_rate = true;

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

class subscriber_sink : public stream_sink<data_message> {
public:
  using super = stream_sink<data_message>;

  using queue_ptr = detail::shared_subscriber_queue_ptr<>;

  subscriber_sink(scheduled_actor* self, subscriber_worker_state* state,
                  queue_ptr qptr, size_t max_qsize)
    : stream_manager(self),
      super(self),
      state_(state),
      queue_(std::move(qptr)),
      max_qsize_(max_qsize) {
    // nop
  }

  bool congested() const noexcept override {
    return queue_->buffer_size() >= max_qsize_;
  }

protected:
   void handle(inbound_path*, downstream_msg::batch& x) override {
    BROKER_TRACE(BROKER_ARG(x));
    using vec_type = std::vector<data_message>;
    if (x.xs.match_elements<vec_type>()) {
      auto& xs = x.xs.get_mutable_as<vec_type>(0);
      auto xs_size = xs.size();
      state_->counter += xs_size;
      queue_->produce(xs_size, std::make_move_iterator(xs.begin()),
                      std::make_move_iterator(xs.end()));
      return;
    }
    BROKER_ERROR("received unexpected batch type (dropped)");
  }

private:
  subscriber_worker_state* state_;
  queue_ptr queue_;
  size_t max_qsize_;
};

behavior subscriber_worker(stateful_actor<subscriber_worker_state>* self,
                           endpoint* ep,
                           detail::shared_subscriber_queue_ptr<> qptr,
                           std::vector<topic> ts, size_t max_qsize) {
  self->send(self * ep->core(), atom::join::value, std::move(ts));
  self->set_default_handler(skip);
  return {
    [=](const endpoint::stream_type& in) {
      BROKER_ASSERT(qptr != nullptr);
      auto mgr = make_counted<subscriber_sink>(self, &self->state, qptr,
                                               max_qsize);
      auto slot = mgr->add_unchecked_inbound_path(in);
      if (slot == invalid_stream_slot) {
        BROKER_WARNING("failed to init stream to subscriber_worker");
        return;
      }
      auto path = mgr->get_inbound_path(slot);
      BROKER_ASSERT(path != nullptr);
      auto slot_at_sender = path->slots.sender;
      self->set_default_handler(print_and_drop);
      self->delayed_send(self, std::chrono::seconds(1), atom::tick::value);
      self->become(
        [=](atom::resume) {
          // TODO: nop ?
          // Triggering the actor should be enough to have it check its mailbox
          // again in order to handle batches from a previously congested
          // manager.
        },
        [=](atom::join a0, atom::update a1, filter_type& f) {
          self->send(ep->core(), a0, a1, slot_at_sender, std::move(f));
        },
        [=](atom::join a0, atom::update a1, filter_type& f, caf::actor& who) {
          self->send(ep->core(), a0, a1, slot_at_sender, std::move(f),
                     std::move(who));
        },
        [=](atom::tick) {
          auto& st = self->state;
          st.tick();
          qptr->rate(st.rate());
          if (st.calculate_rate)
            self->delayed_send(self, std::chrono::seconds(1),
                               atom::tick::value);
        },
        [=](atom::tick, bool x) {
          auto& st = self->state;
          if (st.calculate_rate == x)
            return;
          st.calculate_rate = x;
          if (x)
            self->delayed_send(self, std::chrono::seconds(1),
                               atom::tick::value);
        }
      );
    }
  };
}

} // namespace <anonymous>

subscriber::subscriber(endpoint& e, std::vector<topic> ts, size_t max_qsize)
  : super(max_qsize), ep_(e) {
  BROKER_INFO("creating subscriber for topic(s)" << ts);
  worker_ = ep_.get().system().spawn(subscriber_worker, &ep_.get(), queue_, std::move(ts),
                               max_qsize);
}

subscriber::~subscriber() {
  anon_send_exit(worker_, exit_reason::user_shutdown);
}

size_t subscriber::rate() const {
  return queue_->rate();
}

void subscriber::add_topic(topic x, bool block) {
  BROKER_INFO("adding topic" << x << "to subscriber");
  auto e = filter_.end();
  auto i = std::find(filter_.begin(), e, x);
  if (i == e) {
    filter_.emplace_back(std::move(x));
    if (block) {
      caf::scoped_actor self{ep_.get().system()};
      self->send(worker_, atom::join::value, atom::update::value, filter_, self);
      self->receive([&](bool){});
    } else {
      anon_send(worker_, atom::join::value, atom::update::value, filter_);
    }
  }
}

void subscriber::remove_topic(topic x, bool block) {
  BROKER_INFO("removing topic" << x << "from subscriber");
  auto e = filter_.end();
  auto i = std::find(filter_.begin(), e, x);
  if (i != filter_.end()) {
    filter_.erase(i);
    if (block) {
      caf::scoped_actor self{ep_.get().system()};
      self->send(worker_, atom::join::value, atom::update::value, filter_, self);
      self->receive([&](bool){});
    } else {
      anon_send(worker_, atom::join::value, atom::update::value, filter_);
    }
  }
}

void subscriber::set_rate_calculation(bool x) {
  anon_send(worker_, atom::tick::value, x);
}

void subscriber::became_not_full() {
  anon_send(worker_, atom::resume::value);
}

} // namespace broker
