#pragma once

#include "broker/detail/flare.hh"
#include "broker/detail/native_socket.hh"
#include "broker/message.hh"

#include <caf/async/producer.hpp>
#include <caf/async/spsc_buffer.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/ref_counted.hpp>

#include <mutex>
#include <utility>

namespace broker::internal {

class publisher_queue : public caf::ref_counted, public caf::async::producer {
public:
  using value_type = data_message;

  using buffer_type = caf::async::spsc_buffer<value_type>;

  using buffer_ptr = caf::async::spsc_buffer_ptr<value_type>;

  using guard_type = std::unique_lock<std::mutex>;

  explicit publisher_queue(buffer_ptr buf) : buf_(std::move(buf)) {
    // nop
  }

  buffer_type& buf() {
    return *buf_;
  }

  detail::native_socket fd() const {
    return fx_.fd();
  }

  void wait_on_flare() {
    fx_.await_one();
  }

  void on_consumer_ready() override;

  void on_consumer_cancel() override;

  void on_consumer_demand(size_t demand) override;

  void ref_producer() const noexcept override;

  void deref_producer() const noexcept override;

  size_t demand() const noexcept;

  void push(caf::span<const value_type> items);

  void close();

  friend void intrusive_ptr_add_ref(const publisher_queue* ptr) noexcept {
    ptr->ref();
  }

  friend void intrusive_ptr_release(const publisher_queue* ptr) noexcept {
    ptr->deref();
  };

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

using publisher_queue_ptr = caf::intrusive_ptr<publisher_queue>;

} // namespace broker::internal
