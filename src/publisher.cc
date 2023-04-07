#include "broker/publisher.hh"

#include <future>
#include <numeric>

#include <caf/flow/observable.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/send.hpp>

#include "broker/data.hh"
#include "broker/detail/assert.hh"
#include "broker/detail/flare.hh"
#include "broker/endpoint.hh"
#include "broker/internal/endpoint_access.hh"
#include "broker/internal/logger.hh"
#include "broker/internal/type_id.hh"
#include "broker/message.hh"
#include "broker/topic.hh"

namespace broker::detail {

using broker::internal::facade;
using broker::internal::native;

namespace atom = broker::internal::atom;

struct publisher_queue : public caf::ref_counted, public caf::async::producer {
public:
  using value_type = data_message;

  using buffer_type = caf::async::spsc_buffer<value_type>;

  using buffer_ptr = caf::async::spsc_buffer_ptr<value_type>;

  using guard_type = std::unique_lock<std::mutex>;

  explicit publisher_queue(buffer_ptr buf) : buf_(std::move(buf)) {
    // nop
  }

  ~publisher_queue() override {
    if (buf_)
      buf_->close();
  }

  buffer_type& buf() {
    return *buf_;
  }

  void on_consumer_ready() override {
    BROKER_TRACE("");
  }

  void on_consumer_cancel() override {
    BROKER_TRACE("");
    guard_type guard{mtx_};
    cancelled_ = true;
    if (demand_ == 0)
      fx_.fire();
  }

  void on_consumer_demand(size_t demand) override {
    BROKER_TRACE(BROKER_ARG(demand));
    BROKER_ASSERT(demand > 0);
    guard_type guard{mtx_};
    if (demand_ == 0) {
      demand_ = demand;
      fx_.fire();
    } else {
      demand_ += demand;
    }
  }

  void ref_producer() const noexcept override {
    ref();
  }

  void deref_producer() const noexcept override {
    deref();
  }

  size_t demand() const noexcept {
    guard_type guard{mtx_};
    return demand_;
  }

  auto fd() const {
    return fx_.fd();
  }

  void wait_on_flare() {
    BROKER_TRACE("");
    fx_.await_one();
  }

  void push(caf::span<const value_type> items) {
    BROKER_TRACE(BROKER_ARG2("items.size", items.size()));
    if (items.empty())
      return;
    guard_type guard{mtx_};
    if (cancelled_)
      return;
    while (demand_ == 0) {
      guard.unlock();
      fx_.await_one();
      guard.lock();
      if (cancelled_)
        return;
    }
    if (items.size() < demand_) {
      demand_ -= items.size();
      guard.unlock();
      buf_->push(items);
    } else {
      auto n = demand_;
      demand_ = 0;
      fx_.extinguish();
      guard.unlock();
      buf_->push(items.subspan(0, n));
      push(items.subspan(n));
    }
  }

  friend void intrusive_ptr_add_ref(const publisher_queue* ptr) noexcept {
    ptr->ref();
  }

  friend void intrusive_ptr_release(const publisher_queue* ptr) noexcept {
    ptr->deref();
  }

private:
  /// Provides access to the shared producer-consumer buffer.
  buffer_ptr buf_;

  /// Guards access to other member variables.
  mutable std::mutex mtx_;

  /// Signals to users when data can be read or written.
  mutable detail::flare fx_;

  /// Stores how many demand we currently have from the consumer.
  size_t demand_ = 0;

  /// Stores whether the consumer stopped receiving data.
  bool cancelled_ = false;
};

namespace {

auto* dptr(opaque_type* ptr) {
  return reinterpret_cast<publisher_queue*>(ptr);
}

const auto* dptr(const opaque_type* ptr) {
  return reinterpret_cast<const publisher_queue*>(ptr);
}

auto* dptr(const detail::opaque_ptr& ptr) {
  return dptr(ptr.get());
}

detail::opaque_ptr make_opaque(caf::intrusive_ptr<publisher_queue> ptr) {
  caf::ref_counted* raw = ptr.release();
  return detail::opaque_ptr{reinterpret_cast<detail::opaque_type*>(raw), false};
}

} // namespace

} // namespace broker::detail

using broker::detail::dptr;

namespace broker {

publisher::publisher(detail::opaque_ptr q, topic t)
  : queue_(std::move(q)), topic_(std::move(t)) {
  // nop
}

publisher::~publisher() {
  reset();
}

publisher publisher::make(endpoint& ep, topic t) {
  using caf::async::make_spsc_buffer_resource;
  auto [cons_res, prod_res] = make_spsc_buffer_resource<value_type>();
  caf::anon_send(native(ep.core()), std::move(cons_res));
  auto buf = prod_res.try_open();
  BROKER_ASSERT(buf != nullptr);
  auto qptr = caf::make_counted<detail::publisher_queue>(buf);
  buf->set_producer(qptr);
  return publisher{detail::make_opaque(std::move(qptr)), std::move(t)};
}

size_t publisher::demand() const {
  return dptr(queue_)->demand();
}

size_t publisher::buffered() const {
  return dptr(queue_)->buf().available();
}

size_t publisher::capacity() const {
  return dptr(queue_)->buf().capacity();
}

size_t publisher::free_capacity() const {
  auto x = capacity();
  auto y = buffered();
  return x > y ? x - y : 0;
}

detail::native_socket publisher::fd() const {
  return dptr(queue_)->fd();
}

void publisher::drop_all_on_destruction() {
  drop_on_destruction_ = true;
}

void publisher::publish(data x) {
  auto msg = make_data_message(topic_, std::move(x));
  BROKER_DEBUG("publishing" << msg);
  dptr(queue_)->push(caf::make_span(&msg, 1));
}

void publisher::publish(std::vector<data> xs) {
  std::vector<data_message> msgs;
  msgs.reserve(xs.size());
  for (auto& x : xs)
    msgs.emplace_back(topic_, std::move(x));
#ifdef DEBUG
  BROKER_DEBUG("publishing batch of size" << xs.size());
  for (auto& msg : msgs)
    BROKER_DEBUG("publishing" << msg);
#endif
  dptr(queue_)->push(msgs);
}

void publisher::reset() {
  if (queue_) {
    dptr(queue_)->buf().close();
    queue_.reset();
  }
}

} // namespace broker
