#pragma once

#include "broker/detail/assert.hh"
#include "broker/internal/publisher_queue.hh"
#include "broker/internal/subscriber_queue.hh"
#include "broker/internal/type_id.hh"
#include "broker/logger.hh"
#include "broker/message.hh"
#include "broker/time.hh"

#include <caf/actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/type_id.hpp>

namespace broker::internal {

class hub_impl {
public:
  hub_impl(hub_id id, caf::actor core, subscriber_queue_ptr read_queue,
           publisher_queue_ptr write_queue, filter_type filter = {})
    : id_(id),
      core_(std::move(core)),
      read_queue_(std::move(read_queue)),
      write_queue_(std::move(write_queue)),
      filter_(std::move(filter)) {
    // nop
  }

  ~hub_impl();

  // -- subscriber interface ---------------------------------------------------

  std::vector<data_message> poll();

  data_message get();

  std::vector<data_message> get(size_t num);

  std::vector<data_message> get(size_t num, timestamp timeout);

  data_message get(timestamp timeout);

  size_t available() const noexcept {
    return read_queue_->available();
  }

  detail::native_socket read_fd() const noexcept {
    return read_queue_->fd();
  }

  void subscribe(const topic& what, bool block) {
    auto pred = [&what](const topic& x) { return x == what; };
    if (std::any_of(filter_.begin(), filter_.end(), pred)) {
      return;
    }
    filter_.push_back(what);
    send_filter(block);
  }

  void unsubscribe(const topic& what, bool block) {
    auto pred = [&what](const topic& x) { return x == what; };
    auto i = std::find_if(filter_.begin(), filter_.end(), pred);
    if (i == filter_.end()) {
      return;
    }
    filter_.erase(i);
    send_filter(block);
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

  void publish(data_message&& msg) {
    write_queue_->push(caf::make_span(&msg, 1));
  }

  auto& read_queue() {
    return read_queue_;
  }

private:
  void send_filter(bool block) {
    if (block) {
      caf::scoped_actor self{core_.home_system()};
      self->request(core_, caf::infinite, id_, filter_)
        .receive([] {},
                 [](const caf::error& err) {
                   log::core::debug("update-hub-filter",
                                    "failed to update hub filter: {}",
                                    caf::to_string(err));
                 });
    } else {
      caf::anon_send(core_, id_, filter_);
    }
  }

  hub_id id_;
  caf::actor core_;
  internal::subscriber_queue_ptr read_queue_;
  internal::publisher_queue_ptr write_queue_;
  filter_type filter_;
};

} // namespace broker::internal
