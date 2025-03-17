#pragma once

#include "broker/detail/assert.hh"
#include "broker/internal/publisher_queue.hh"
#include "broker/internal/subscriber_queue.hh"
#include "broker/message.hh"
#include "broker/time.hh"

#include <caf/actor.hpp>

namespace broker::internal {

class hub_impl {
public:
  hub_impl(hub_id id, caf::actor core, subscriber_queue_ptr read_queue,
           publisher_queue_ptr write_queue)
    : id_(id),
      core_(std::move(core)),
      read_queue_(std::move(read_queue)),
      write_queue_(std::move(write_queue)) {
    // nop
  }

  // -- subscriber interface ---------------------------------------------------

  std::vector<data_message> poll();

  data_message get();

  std::vector<data_message> get(size_t num);

  data_message get(timestamp timeout);

  size_t available() const noexcept {
    return read_queue_->available();
  }

  detail::native_socket read_fd() const noexcept {
    return read_queue_->fd();
  }

  void subscribe(const topic&, bool) {
    // TODO
  }

  void unsubscribe(const topic&, bool) {
    // TODO
  }

  // -- publisher interface ----------------------------------------------------

  size_t demand() const {
    return write_queue_->demand();
  }

  size_t buffered() const {
    return write_queue_->buf().available();
  }

  size_t capacity() const {
    return write_queue_->buf().capacity();
  }

  detail::native_socket write_fd() const noexcept {
    return write_queue_->fd();
  }

  void publish(const topic& dst, data_message&& msg) {
    write_queue_->push(caf::make_span(&msg, 1));
  }

private:
  hub_id id_;
  caf::actor core_;
  internal::subscriber_queue_ptr read_queue_;
  internal::publisher_queue_ptr write_queue_;
};

} // namespace broker::internal
