#include "broker/publisher.hh"

#include "broker/builder.hh"
#include "broker/data.hh"
#include "broker/detail/assert.hh"
#include "broker/endpoint.hh"
#include "broker/hub.hh"
#include "broker/internal/endpoint_access.hh"
#include "broker/internal/fwd.hh"
#include "broker/internal/hub_impl.hh"
#include "broker/internal/native.hh"
#include "broker/internal/publisher_queue.hh"
#include "broker/internal/type_id.hh"
#include "broker/logger.hh"
#include "broker/message.hh"
#include "broker/topic.hh"

#include <caf/actor.hpp>
#include <caf/async/spsc_buffer.hpp>
#include <caf/scoped_actor.hpp>

#include <future>
#include <numeric>

using namespace std::literals;

namespace atom = broker::internal::atom;

namespace broker {

publisher::publisher(topic dst, std::shared_ptr<internal::hub_impl> ptr)
  : dst_(std::move(dst)), impl_(std::move(ptr)) {
  // nop
}

publisher::~publisher() {
  // nop; must be out-of-line to avoid header dependencies.
}

publisher publisher::make(endpoint& ep, topic dst) {
  using caf::async::make_spsc_buffer_resource;
  auto id = hub::next_id();
  // Produce the queue for the hub.
  auto [src, snk] = make_spsc_buffer_resource<data_message>();
  // Use the queue for writing.
  auto pub_buf = snk.try_open();
  BROKER_ASSERT(pub_buf != nullptr);
  auto pub = caf::make_counted<internal::publisher_queue>(pub_buf);
  pub_buf->set_producer(pub);
  // Connect the buffer to the core.
  auto& core = internal::native(ep.core());
  auto& sys = internal::endpoint_access{&ep}.sys();
  caf::scoped_actor self{sys};
  self
    ->request(core, 2s, id, filter_type{}, true, std::move(src),
              internal::data_producer_res{})
    .receive(
      [] {
        // OK, the core has completed the setup.
      },
      [](const caf::error& what) {
        log::core::error("cannot-create-hub", "failed to create hub: {}", what);
        throw std::runtime_error("cannot create hub");
      });
  // Wrap the queues in shared pointers and create the hub.
  return {std::move(dst),
          std::make_shared<internal::hub_impl>(id, core, nullptr, pub)};
}

size_t publisher::demand() const {
  return impl_->demand();
}

size_t publisher::buffered() const {
  return impl_->buffered();
}

size_t publisher::capacity() const {
  return impl_->capacity();
}

size_t publisher::free_capacity() const {
  auto x = capacity();
  auto y = buffered();
  return x > y ? x - y : 0;
}

detail::native_socket publisher::fd() const {
  return impl_->write_fd();
}

void publisher::drop_all_on_destruction() {
  // nop
}

void publisher::publish(const data& val) {
  impl_->publish(make_data_message(dst_, val));
}

void publisher::publish(const std::vector<data>& vals) {
  for (auto& val : vals) {
    impl_->publish(make_data_message(dst_, val));
  }
}

void publisher::publish(set_builder&& content) {
  impl_->publish(std::move(content).build_envelope(dst_.string()));
}

void publisher::publish(table_builder&& content) {
  impl_->publish(std::move(content).build_envelope(dst_.string()));
}

void publisher::publish(list_builder&& content) {
  impl_->publish(std::move(content).build_envelope(dst_.string()));
}

void publisher::reset() {
  if (impl_) {
    impl_ = nullptr;
  }
}

} // namespace broker
