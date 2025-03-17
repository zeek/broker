#include "broker/internal/subscriber_queue.hh"

#include "broker/detail/assert.hh"
#include "broker/logger.hh"

namespace broker::internal {

subscriber_queue::subscriber_queue(buffer_ptr buf) : buf_(std::move(buf)) {
  // nop
}

subscriber_queue::~subscriber_queue() {
  if (buf_)
    buf_->cancel();
}

void subscriber_queue::on_producer_ready() {
  // nop
}

void subscriber_queue::on_producer_wakeup() {
  guard_type guard{mtx_};
  if (!ready_) {
    fx_.fire();
    ready_ = true;
  }
}

void subscriber_queue::wait() {
  guard_type guard{mtx_};
  while (!ready_) {
    guard.unlock();
    fx_.await_one();
    guard.lock();
  }
}

bool subscriber_queue::wait_until(timestamp abs_timeout) {
  guard_type guard{mtx_};
  while (!ready_) {
    guard.unlock();
    if (!fx_.await_one(abs_timeout)) {
      guard.lock();
      return ready_;
    }
    guard.lock();
  }
  return true;
}

void subscriber_queue::ref_consumer() const noexcept {
  ref();
}

void subscriber_queue::deref_consumer() const noexcept {
  deref();
}

detail::native_socket subscriber_queue::fd() const noexcept {
  return fx_.fd();
}

void subscriber_queue::cancel() {
  if (buf_)
    buf_->cancel();
}

void subscriber_queue::extinguish() {
  guard_type guard{mtx_};
  if (ready_) {
    ready_ = false;
    fx_.extinguish();
  }
}

bool subscriber_queue::pull(data_message& dst) {
  struct cb {
    subscriber_queue* qptr;
    data_message* dst;
    void on_next(const data_message& val) {
      *dst = val;
    }
    void on_complete() {
      qptr->extinguish();
    }
    void on_error(const caf::error&) {
      qptr->extinguish();
    }
  };
  using caf::async::delay_errors;
  cb consumer{this, &dst};
  if (buf_) {
    auto [open, n] = buf_->pull(delay_errors, 1, consumer);
    log::endpoint::debug("subscriber-pull",
                         "got {} messages from bounded buffer", n);
    if (!open) {
      log::endpoint::debug("subscriber-queue-closed",
                           "nothing left to pull, queue closed");
      buf_ = nullptr;
      return false;
    }
    if (buf_->available() == 0) {
      // Note: We always *must* acquire the lock on the buffer before
      // acquiring the lock on the subscriber to prevent deadlocks.
      guard_type buf_guard{buf_->mtx()};
      guard_type sub_guard{mtx_};
      if (ready_ && buf_->available_unsafe() == 0) {
        ready_ = false;
        fx_.extinguish();
      }
      return true;
    }
    return true;
  }
  log::endpoint::debug("subscriber-queue-closed",
                       "nothing left to pull, queue closed");
  return false;
}

bool subscriber_queue::pull(std::vector<data_message>& dst, size_t num) {
  BROKER_ASSERT(num > 0);
  BROKER_ASSERT(dst.size() < num);
  struct cb {
    subscriber_queue* qptr;
    std::vector<data_message>* dst;
    void on_next(const data_message& val) {
      dst->push_back(val);
    }
    void on_complete() {
      qptr->extinguish();
    }
    void on_error(const caf::error&) {
      qptr->extinguish();
    }
  };
  using caf::async::delay_errors;
  cb consumer{this, &dst};
  if (buf_) {
    auto [open, n] = buf_->pull(delay_errors, num - dst.size(), consumer);
    log::endpoint::debug("subscriber-pull",
                         "got {} messages from bounded buffer", n);
    if (!open) {
      log::endpoint::debug("subscriber-queue-closed",
                           "nothing left to pull, queue closed");
      buf_ = nullptr;
      return false;
    }
    if (buf_->available() == 0) {
      // Note: We always *must* acquire the lock on the buffer before
      // acquiring the lock on the subscriber to prevent deadlocks.
      guard_type buf_guard{buf_->mtx()};
      guard_type sub_guard{mtx_};
      if (ready_ && buf_->available_unsafe() == 0) {
        ready_ = false;
        fx_.extinguish();
      }
      return true;
    }
    return true;
  }
  log::endpoint::debug("subscriber-queue-closed",
                       "nothing left to pull, queue closed");
  return false;
}

size_t subscriber_queue::capacity() const noexcept {
  return buf_ ? buf_->capacity() : size_t{0};
}

size_t subscriber_queue::available() const noexcept {
  return buf_ ? buf_->available() : size_t{0};
}

void intrusive_ptr_add_ref(const subscriber_queue* ptr) noexcept {
  ptr->ref();
}

void intrusive_ptr_release(const subscriber_queue* ptr) noexcept {
  ptr->deref();
}

} // namespace broker::internal
