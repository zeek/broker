#include "broker/subscriber.hh"

#include <cstddef>
#include <future>
#include <numeric>
#include <stdexcept>
#include <utility>

#include <caf/async/consumer.hpp>
#include <caf/async/spsc_buffer.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/scheduled_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>
#include <caf/stateful_actor.hpp>

#include "broker/detail/assert.hh"
#include "broker/endpoint.hh"
#include "broker/filter_type.hh"
#include "broker/internal/native.hh"
#include "broker/internal/subscriber_queue.hh"
#include "broker/internal/type_id.hh"
#include "broker/logger.hh"

using broker::internal::native;

namespace broker::detail {

namespace {

auto* dptr(detail::opaque_type* ptr) {
  auto bptr = reinterpret_cast<caf::ref_counted*>(ptr);
  return static_cast<internal::subscriber_queue*>(bptr);
}

auto* dptr(const detail::opaque_ptr& ptr) {
  return dptr(ptr.get());
}

detail::opaque_ptr
make_opaque(caf::intrusive_ptr<internal::subscriber_queue> ptr) {
  caf::ref_counted* raw = ptr.release();
  return detail::opaque_ptr{reinterpret_cast<detail::opaque_type*>(raw), false};
}

} // namespace

} // namespace broker::detail

using broker::detail::dptr;

namespace broker {

subscriber::subscriber(detail::opaque_ptr queue,
                       std::shared_ptr<filter_type> filter, worker core)
  : queue_(std::move(queue)),
    core_(std::move(core)),
    core_filter_(std::move(filter)) {
  // nop
}

subscriber::~subscriber() {
  reset();
}

subscriber subscriber::make(endpoint& ep, filter_type filter, size_t) {
  log::endpoint::info("creating-subscriber",
                      "creating subscriber for topic(s): {}", filter);
  using caf::async::make_spsc_buffer_resource;
  auto fptr = std::make_shared<filter_type>(std::move(filter));
  auto [con_res, prod_res] = make_spsc_buffer_resource<data_message>();
  caf::anon_send(native(ep.core()), fptr, std::move(prod_res));
  auto buf = con_res.try_open();
  BROKER_ASSERT(buf != nullptr);
  auto qptr = caf::make_counted<internal::subscriber_queue>(buf);
  buf->set_consumer(qptr);
  return subscriber{detail::make_opaque(std::move(qptr)), std::move(fptr),
                    ep.core()};
}

data_message subscriber::get() {
  data_message msg;
  if (!dptr(queue_)->pull(msg)) {
    throw std::runtime_error("subscriber queue closed");
  }
  return msg;
}

std::vector<data_message> subscriber::get(size_t num) {
  BROKER_ASSERT(num > 0);
  auto q = dptr(queue_);
  std::vector<data_message> buf;
  buf.reserve(num);
  q->pull(buf, num);
  while (buf.size() < num) {
    wait();
    if (!q->pull(buf, num))
      return buf;
  }
  return buf;
}

std::vector<data_message> subscriber::do_get(size_t num,
                                             timestamp abs_timeout) {
  std::vector<data_message> buf;
  do_get(buf, num, abs_timeout);
  return buf;
}

void subscriber::do_get(std::vector<data_message>& buf, size_t num,
                        timestamp abs_timeout) {
  auto q = dptr(queue_);
  buf.clear();
  buf.reserve(num);
  q->pull(buf, num);
  while (buf.size() < num && wait_until(abs_timeout))
    q->pull(buf, num);
}

std::vector<data_message> subscriber::poll() {
  // The Queue may return a capacity of 0 if the producer has closed the flow.
  std::vector<data_message> buf;
  auto q = dptr(queue_);
  auto max_size = q->capacity();
  if (max_size > 0) {
    buf.reserve(max_size);
    q->pull(buf, max_size);
  }
  return buf;
}

size_t subscriber::available() const noexcept {
  return dptr(queue_)->available();
}

detail::native_socket subscriber::fd() const noexcept {
  return dptr(queue_)->fd();
}

void subscriber::add_topic(topic x, bool block) {
  log::endpoint::info("subscriber-add-topic", "add topic {} to subscriber", x);
  update_filter(std::move(x), true, block);
}

void subscriber::remove_topic(topic x, bool block) {
  log::endpoint::info("subscriber-remove-topic",
                      "remove topic {} from subscriber", x);
  update_filter(std::move(x), false, block);
}

void subscriber::reset() {
  if (queue_) {
    dptr(queue_)->cancel();
    queue_ = nullptr;
    core_ = nullptr;
  }
}

void subscriber::update_filter(topic what, bool add, bool block) {
  using internal::native;
  if (!block) {
    caf::anon_send(native(core_), core_filter_, std::move(what), add,
                   std::shared_ptr<std::promise<void>>{nullptr});
  } else {
    auto sync = std::make_shared<std::promise<void>>();
    auto vfut = sync->get_future();
    caf::anon_send(native(core_), core_filter_, std::move(what), add,
                   std::move(sync));
    vfut.get();
  }
}

void subscriber::wait() {
  dptr(queue_)->wait();
}

bool subscriber::wait_for(timespan rel_timeout) {
  return wait_until(now() + rel_timeout);
}

bool subscriber::wait_until(timestamp abs_timeout) {
  return dptr(queue_)->wait_until(abs_timeout);
}

} // namespace broker
