#pragma once

#include "broker/detail/flare.hh"
#include "broker/message.hh"

#include <caf/async/consumer.hpp>
#include <caf/async/spsc_buffer.hpp>
#include <caf/ref_counted.hpp>

namespace broker::internal {

struct subscriber_queue : public caf::ref_counted, public caf::async::consumer {
public:
  using buffer_type = caf::async::spsc_buffer<data_message>;

  using buffer_ptr = caf::async::spsc_buffer_ptr<data_message>;

  using guard_type = std::unique_lock<std::mutex>;

  explicit subscriber_queue(buffer_ptr buf);

  ~subscriber_queue() override;

  void on_producer_ready() override;

  void on_producer_wakeup() override;

  void wait();

  bool wait_until(timestamp abs_timeout);

  void ref_consumer() const noexcept override;

  void deref_consumer() const noexcept override;

  auto fd() const noexcept;

  void cancel();

  void extinguish();

  bool pull(std::vector<data_message>& dst, size_t num);

  size_t capacity() const noexcept;

  size_t available() const noexcept;

  friend void intrusive_ptr_add_ref(const subscriber_queue* ptr) noexcept;

  friend void intrusive_ptr_release(const subscriber_queue* ptr) noexcept;

private:
  /// Provides access to the shared buffer.
  buffer_ptr buf_;

  /// Guards access to other member variables.
  mutable std::mutex mtx_;

  /// Signals to users when data can be read or written.
  mutable detail::flare fx_;

  /// Stores whether we have data available.
  bool ready_ = false;
};

} // namespace broker::internal
