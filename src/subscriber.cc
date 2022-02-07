#include "broker/subscriber.hh"

#include <cstddef>
#include <utility>
#include <chrono>
#include <numeric>

#include <caf/event_based_actor.hpp>
#include <caf/scheduled_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>
#include <caf/stateful_actor.hpp>

#include "broker/detail/assert.hh"
#include "broker/endpoint.hh"
#include "broker/filter_type.hh"
#include "broker/internal/endpoint_access.hh"
#include "broker/internal/logger.hh"
#include "broker/internal/type_id.hh"

using broker::internal::facade;
using broker::internal::native;

namespace atom = broker::internal::atom;

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

class subscriber_sink : public caf::stream_sink<data_message> {
public:
  using super = caf::stream_sink<data_message>;

  using queue_ptr = detail::shared_subscriber_queue_ptr<>;

  subscriber_sink(caf::scheduled_actor* self, subscriber_worker_state* state,
                  queue_ptr qptr, size_t max_qsize)
    : stream_manager(self),
      super(self),
      state_(state),
      queue_(std::move(qptr)),
      max_qsize_(max_qsize) {
    // nop
  }

  using super::congested;

  bool congested(const caf::inbound_path&) const noexcept override {
    return queue_->buffer_size() >= max_qsize_;
  }

protected:
   void handle(caf::inbound_path*, caf::downstream_msg::batch& x) override {
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

caf::behavior
subscriber_worker(caf::stateful_actor<subscriber_worker_state>* self,
                  endpoint* ep, detail::shared_subscriber_queue_ptr<> qptr,
                  std::vector<topic> ts, size_t max_qsize) {
  self->send(self * native(ep->core()), atom::join_v, std::move(ts));
  self->set_default_handler(caf::skip);
  return {
    [=](caf::stream<data_message> in) {
      BROKER_ASSERT(qptr != nullptr);
      auto mgr = caf::make_counted<subscriber_sink>(self, &self->state, qptr,
                                                    max_qsize);
      auto slot = mgr->add_unchecked_inbound_path(in);
      if (slot == caf::invalid_stream_slot) {
        BROKER_WARNING("failed to init stream to subscriber_worker");
        return;
      }
      auto path = mgr->get_inbound_path(slot);
      BROKER_ASSERT(path != nullptr);
      auto slot_at_sender = path->slots.sender;
      self->set_default_handler(caf::print_and_drop);
      self->delayed_send(self, std::chrono::seconds(1), atom::tick_v);
      self->become(
        [=](atom::resume) {
          // TODO: nop ?
          // Triggering the actor should be enough to have it check its mailbox
          // again in order to handle batches from a previously congested
          // manager.
        },
        [=](atom::join a0, atom::update a1, filter_type& f) {
          self->send(native(ep->core()), a0, a1, slot_at_sender, std::move(f));
        },
        [=](atom::join a0, atom::update a1, filter_type& f, caf::actor& who) {
          self->send(native(ep->core()), a0, a1, slot_at_sender, std::move(f),
                     std::move(who));
        },
        [=](atom::tick) {
          auto& st = self->state;
          st.tick();
          qptr->rate(st.rate());
          if (st.calculate_rate)
            self->delayed_send(self, std::chrono::seconds(1),
                               atom::tick_v);
        },
        [=](atom::tick, bool x) {
          auto& st = self->state;
          if (st.calculate_rate == x)
            return;
          st.calculate_rate = x;
          if (x)
            self->delayed_send(self, std::chrono::seconds(1),
                               atom::tick_v);
        });
    }};
}

} // namespace <anonymous>

subscriber::subscriber(endpoint& ep, std::vector<topic> ts, size_t max_qsize)
  : super(max_qsize), filter_(std::move(ts)), ep_(&ep) {
  BROKER_INFO("creating subscriber for topic(s)" << filter_);
  auto& sys = internal::endpoint_access{ep_}.sys();
  auto hdl = sys.spawn(subscriber_worker, ep_, queue_, filter_, max_qsize);
  worker_ = facade(hdl);
}

subscriber::~subscriber() {
  if (worker_)
    caf::anon_send_exit(native(worker_), caf::exit_reason::user_shutdown);
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
      caf::scoped_actor self{internal::endpoint_access{ep_}.sys()};
      self->send(native(worker_), atom::join_v, atom::update_v, filter_, self);
      self->receive([&](bool){});
    } else {
      anon_send(native(worker_), atom::join_v, atom::update_v, filter_);
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
      caf::scoped_actor self{internal::endpoint_access{ep_}.sys()};
      self->send(native(worker_), atom::join_v, atom::update_v, filter_, self);
      self->receive([&](bool){});
    } else {
      anon_send(native(worker_), atom::join_v, atom::update_v, filter_);
    }
  }
}

void subscriber::set_rate_calculation(bool x) {
  caf::anon_send(native(worker_), atom::tick_v, x);
}

void subscriber::became_not_full() {
  caf::anon_send(native(worker_), atom::resume_v);
}

void subscriber::reset() {
  if (!worker_)
    return;
  caf::anon_send_exit(native(worker_), caf::exit_reason::user_shutdown);
  worker_ = nullptr;
}

} // namespace broker
