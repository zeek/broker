#include "broker/publisher.hh"

#include <future>
#include <numeric>

#include "broker/builder.hh"
#include "broker/data.hh"
#include "broker/detail/assert.hh"
#include "broker/endpoint.hh"
#include "broker/internal/native.hh"
#include "broker/internal/publisher_queue.hh"
#include "broker/internal/type_id.hh"
#include "broker/logger.hh"
#include "broker/message.hh"
#include "broker/topic.hh"

#include <caf/flow/observable.hpp>
#include <caf/send.hpp>

namespace atom = broker::internal::atom;

namespace broker {

publisher::publisher(internal::publisher_queue* q, topic t)
  : queue_(q), topic_(std::move(t)) {
  // nop
}

publisher::publisher(publisher&& other) noexcept
  : queue_(nullptr), topic_(std::move(other.topic_)) {
  std::swap(queue_, other.queue_);
}

publisher& publisher::operator=(publisher&& other) noexcept {
  if (this != &other) {
    std::swap(queue_, other.queue_);
    std::swap(topic_, other.topic_);
  }
  return *this;
}

publisher::~publisher() {
  if (queue_ != nullptr) {
    intrusive_ptr_release(queue_);
  }
}

publisher publisher::make(endpoint& ep, topic t) {
  using caf::async::make_spsc_buffer_resource;
  auto [cons_res, prod_res] = make_spsc_buffer_resource<value_type>();
  caf::anon_send(internal::native(ep.core()), std::move(cons_res));
  auto buf = prod_res.try_open();
  BROKER_ASSERT(buf != nullptr);
  auto* qptr = new internal::publisher_queue(buf);
  buf->set_producer(qptr);
  return publisher{qptr, std::move(t)};
}

size_t publisher::demand() const {
  return queue_->demand();
}

size_t publisher::buffered() const {
  return queue_->buf().available();
}

size_t publisher::capacity() const {
  return queue_->buf().capacity();
}

size_t publisher::free_capacity() const {
  auto x = capacity();
  auto y = buffered();
  return x > y ? x - y : 0;
}

detail::native_socket publisher::fd() const {
  return queue_->fd();
}

void publisher::drop_all_on_destruction() {
  drop_on_destruction_ = true;
}

void publisher::publish(const data& x) {
  auto msg = make_data_message(topic_, x);
  log::endpoint::debug("publish", "publishing {}", msg);
  queue_->push(caf::make_span(&msg, 1));
}

void publisher::publish(const std::vector<data>& xs) {
  std::vector<data_message> msgs;
  msgs.reserve(xs.size());
  for (auto& x : xs)
    msgs.push_back(make_data_message(topic_, x));
  log::endpoint::debug("publish-batch", "publishing a batch of size {}",
                       xs.size());
  queue_->push(msgs);
}

void publisher::publish(set_builder&& x) {
  auto msg = std::move(x).build_envelope(topic_.string());
  queue_->push(caf::make_span(&msg, 1));
}

void publisher::publish(table_builder&& x) {
  auto msg = std::move(x).build_envelope(topic_.string());
  queue_->push(caf::make_span(&msg, 1));
}

void publisher::publish(list_builder&& x) {
  auto msg = std::move(x).build_envelope(topic_.string());
  queue_->push(caf::make_span(&msg, 1));
}

void publisher::reset() {
  if (queue_ != nullptr) {
    queue_->buf().close();
    intrusive_ptr_release(queue_);
    queue_ = nullptr;
  }
}

} // namespace broker
