#include "broker/hub.hh"

#include "broker/builder.hh"
#include "broker/detail/assert.hh"
#include "broker/endpoint.hh"
#include "broker/fwd.hh"
#include "broker/internal/endpoint_access.hh"
#include "broker/internal/hub_impl.hh"
#include "broker/internal/native.hh"
#include "broker/internal/publisher_queue.hh"
#include "broker/internal/subscriber_queue.hh"
#include "broker/internal/type_id.hh"
#include "broker/logger.hh"
#include "broker/message.hh"

#include <caf/actor.hpp>
#include <caf/async/spsc_buffer.hpp>
#include <caf/scoped_actor.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>

using namespace std::literals;

namespace broker {

namespace {

/// Stores the last ID that was assigned to a hub. The next ID will be one
/// greater than this value. Hence, valid hub IDs start at 1.
std::atomic<uint64_t> last_hub_id = 0;

} // namespace

// --- static utility functions ------------------------------------------------

hub_id hub::next_id() noexcept {
  return static_cast<hub_id>(++last_hub_id);
}

// --- constructors, destructors, and assignment operators ---------------------

hub::hub(std::shared_ptr<internal::hub_impl> ptr) : impl_(std::move(ptr)) {
  // nop
}

hub::~hub() {
  // nop; must be out-of-line to avoid header dependencies.
}

hub hub::make(endpoint& ep, filter_type filter) {
  using caf::async::make_spsc_buffer_resource;
  auto id = next_id();
  // Produce the two queues for the hub.
  auto [src1, snk1] = make_spsc_buffer_resource<data_message>();
  auto [src2, snk2] = make_spsc_buffer_resource<data_message>();
  // Use queue 1 for reading.
  auto sub_buf = src1.try_open();
  BROKER_ASSERT(sub_buf != nullptr);
  auto sub = caf::make_counted<internal::subscriber_queue>(sub_buf);
  sub_buf->set_consumer(sub);
  // Use queue 2 for writing.
  auto pub_buf = snk2.try_open();
  BROKER_ASSERT(pub_buf != nullptr);
  auto pub = caf::make_counted<internal::publisher_queue>(pub_buf);
  pub_buf->set_producer(pub);
  // Connect the buffers to the core.
  auto& core = internal::native(ep.core());
  auto& sys = internal::endpoint_access{&ep}.sys();
  caf::scoped_actor self{sys};
  self->request(core, 2s, id, filter, false, std::move(src2), std::move(snk1))
    .receive(
      [] {
        // OK, the core has completed the setup.
      },
      [](const caf::error& what) {
        log::core::error("cannot-create-hub", "failed to create hub: {}",
                         caf::to_string(what));
        throw std::runtime_error("cannot create hub");
      });
  // Wrap the queues in shared pointers and create the hub.
  return hub(std::make_shared<internal::hub_impl>(id, core, sub, pub,
                                                  std::move(filter)));
}

// --- accessors ---------------------------------------------------------------

size_t hub::available() const noexcept {
  return impl_->available();
}

size_t hub::demand() const {
  return impl_->demand();
}

size_t hub::buffered() const {
  return impl_->buffered();
}

size_t hub::capacity() const {
  return impl_->capacity();
}

detail::native_socket hub::read_fd() const noexcept {
  return impl_->read_fd();
}

detail::native_socket hub::write_fd() const noexcept {
  return impl_->write_fd();
}

std::vector<data_message> hub::poll() {
  return impl_->poll();
}

data_message hub::get() {
  return impl_->get();
}

std::vector<data_message> hub::get(size_t num) {
  return impl_->get(num);
}

data_message hub::do_get(timespan timeout) {
  return do_get(timestamp::clock::now() + timeout);
}

data_message hub::do_get(timestamp timeout) {
  return impl_->get(timeout);
}

void hub::subscribe(const topic& x, bool block) {
  impl_->subscribe(x, block);
}

void hub::unsubscribe(const topic& x, bool block) {
  impl_->unsubscribe(x, block);
}

void hub::publish(data_message msg) {
  impl_->publish(std::move(msg));
}

void hub::publish(const topic& dest, set_builder&& content) {
  impl_->publish(std::move(content).build_envelope(dest.string()));
}

void hub::publish(const topic& dest, table_builder&& content) {
  impl_->publish(std::move(content).build_envelope(dest.string()));
}

void hub::publish(const topic& dest, list_builder&& content) {
  impl_->publish(std::move(content).build_envelope(dest.string()));
}

} // namespace broker
