#include "broker/logger.hh" // Must come before any CAF include.
#include "broker/publisher.hh"

#include <numeric>

#include <caf/attach_stream_source.hpp>
#include <caf/flow/observable.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/send.hpp>

#include "broker/data.hh"
#include "broker/endpoint.hh"
#include "broker/message.hh"
#include "broker/topic.hh"

using namespace caf;

namespace broker {

namespace {

// TODO: make these constants configurable

/// Defines how many seconds are averaged for the computation of the send rate.
constexpr size_t sample_size = 10;

/// Defines how many items are stored in the queue.
constexpr size_t queue_size = 30;

using queue_ptr = detail::shared_publisher_queue_ptr<>;

using buffered_observable_impl
  = caf::flow::buffered_observable_impl<data_message>;

using buffered_observable_ptr = caf::intrusive_ptr<buffered_observable_impl>;

struct publisher_worker_state {
  caf::event_based_actor* self;
  buffered_observable_ptr ptr;
  bool shutting_down = false;

  static inline const char* name = "broker.publisher";

  explicit publisher_worker_state(caf::event_based_actor* self) : self(self) {
    // nop
  }

  caf::behavior make_behavior() {
    return {
      [=](atom::resume) {
        if (ptr)
          ptr->try_push();
      },
      [=](atom::shutdown) {
        shutting_down = true;
        self->unbecome();
        if (ptr)
          ptr->try_push();
      },
    };
  }
};

using publisher_worker_actor = caf::stateful_actor<publisher_worker_state>;

class queue_reader {
public:
  using output_type = data_message;

  queue_reader(queue_ptr ptr, publisher_worker_state* state)
    : ptr_(std::move(ptr)), state_(state) {
    // nop
  }

  queue_reader(queue_reader&&) = default;
  queue_reader(const queue_reader&) = default;
  queue_reader& operator=(queue_reader&&) = default;
  queue_reader& operator=(const queue_reader&) = default;

  template <class Step, class... Steps>
  void pull(size_t n, Step& step, Steps&... steps) {
    auto m = std::max(last_pull_size_, n);
    auto consumed = ptr_->consume(m, [&](const data_message& item) {
      // TODO: on_next may return false, in which case we are supposed to stop
      //       early. However, `consume` does not support this yet.
      std::ignore = step.on_next(item, steps...);
    });
    last_pull_size_ = m - consumed;
    if (consumed == 0 && state_->shutting_down)
      step.on_complete(steps...);
  }

private:
  // When calling consume again after an unsuccessful run, we must ask for *at
  // least* as many items on the queue again (see
  // shared_publisher_queue::consume).
  size_t last_pull_size_ = 0;

  queue_ptr ptr_;

  publisher_worker_state* state_;
};

} // namespace <anonymous>

publisher::publisher(endpoint& ep, topic t)
  : drop_on_destruction_(false),
    queue_(detail::make_shared_publisher_queue(queue_size)),
    topic_(std::move(t)) {
  auto src = caf::async::publisher_from<publisher_worker_actor>(
    ep.system(), [this](auto* self) {
      worker_ = self;
      return self->make_observable().lift(queue_reader{queue_, &(self->state)});
    });
}

publisher::~publisher() {
  reset();
}

size_t publisher::demand() const {
  return static_cast<size_t>(queue_->pending());
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

size_t publisher::send_rate() const {
  return static_cast<size_t>(queue_->rate());
}

void publisher::drop_all_on_destruction() {
  drop_on_destruction_ = true;
}

void publisher::publish(data x) {
  BROKER_INFO("publishing" << std::make_pair(topic_, x));
  if (queue_->produce(topic_, std::move(x)))
    anon_send(worker_, atom::resume_v);
}

void publisher::publish(std::vector<data> xs) {
  auto t = static_cast<ptrdiff_t>(queue_->capacity());
  auto i = xs.begin();
  auto e = xs.end();
  while (i != e) {
    auto j = i + std::min(std::distance(i, e), t);
#ifdef DEBUG
    BROKER_INFO("publishing batch of size" << (j - i));
    for (auto l = i; l < j; l++) {
      BROKER_INFO("publishing" << std::make_pair(topic_, *l));
    }
#endif
    if (queue_->produce(topic_, i, j))
      anon_send(worker_, atom::resume_v);
    i = j;
  }
}

void publisher::reset() {
  if (!worker_)
    return;
  if (!drop_on_destruction_)
    anon_send(worker_, atom::shutdown_v);
  else
    anon_send_exit(worker_, exit_reason::user_shutdown);
  worker_ = nullptr;
}

} // namespace broker
