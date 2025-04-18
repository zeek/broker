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
#include "broker/hub.hh"
#include "broker/internal/endpoint_access.hh"
#include "broker/internal/hub_impl.hh"
#include "broker/internal/native.hh"
#include "broker/internal/subscriber_queue.hh"
#include "broker/internal/type_id.hh"
#include "broker/logger.hh"

using namespace std::literals;

namespace broker {

subscriber::subscriber(std::shared_ptr<internal::hub_impl> ptr)
  : impl_(std::move(ptr)) {
  // nop
}

subscriber::~subscriber() {
  // nop; must be out-of-line to avoid header dependencies.
}

subscriber subscriber::make(endpoint& ep, filter_type filter, size_t) {
  using caf::async::make_spsc_buffer_resource;
  auto id = hub::next_id();
  // Produce the queue for the hub.
  auto [src, snk] = make_spsc_buffer_resource<data_message>();
  // Use the queue for reading.
  auto sub_buf = src.try_open();
  BROKER_ASSERT(sub_buf != nullptr);
  auto sub = caf::make_counted<internal::subscriber_queue>(sub_buf);
  sub_buf->set_consumer(sub);
  // Connect the buffers to the core.
  auto& core = internal::native(ep.core());
  auto& sys = internal::endpoint_access{&ep}.sys();
  caf::scoped_actor self{sys};
  self
    ->request(core, 2s, id, filter, true, internal::data_consumer_res{},
              std::move(snk))
    .receive(
      [] {
        // OK, the core has completed the setup.
      },
      [](const caf::error& what) {
        log::core::error("cannot-create-hub", "failed to create hub: {}", what);
        throw std::runtime_error("cannot create hub");
      });
  // Wrap the queues in shared pointers and create the hub.
  return subscriber{std::make_shared<internal::hub_impl>(id, core, sub, nullptr,
                                                         std::move(filter))};
}

data_message subscriber::get() {
  return impl_->get();
}

std::vector<data_message> subscriber::get(size_t num) {
  return impl_->get(num);
}

std::vector<data_message> subscriber::do_get(size_t num,
                                             timestamp abs_timeout) {
  return impl_->get(num, abs_timeout);
}

void subscriber::do_get(std::vector<data_message>& buf, size_t num,
                        timestamp abs_timeout) {
  buf = impl_->get(num, abs_timeout);
}

std::vector<data_message> subscriber::poll() {
  return impl_->poll();
}

size_t subscriber::available() const noexcept {
  return impl_->available();
}

detail::native_socket subscriber::fd() const noexcept {
  return impl_->read_fd();
}

void subscriber::add_topic(topic x, bool block) {
  log::endpoint::info("subscriber-add-topic", "add topic {} to subscriber", x);
  impl_->subscribe(x, block);
}

void subscriber::remove_topic(topic x, bool block) {
  log::endpoint::info("subscriber-remove-topic",
                      "remove topic {} from subscriber", x);
  impl_->unsubscribe(x, block);
}

void subscriber::reset() {
  impl_ = nullptr;
}

void subscriber::wait() {
  impl_->read_queue()->wait();
}

bool subscriber::wait_for(timespan rel_timeout) {
  return wait_until(now() + rel_timeout);
}

bool subscriber::wait_until(timestamp abs_timeout) {
  return impl_->read_queue()->wait_until(abs_timeout);
}

} // namespace broker
