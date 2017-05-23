#ifndef BROKER_DETAIL_SHARED_QUEUE_HH
#define BROKER_DETAIL_SHARED_QUEUE_HH

#include <atomic>
#include <deque>
#include <mutex>
#include <thread>
#include <condition_variable>

#include <caf/intrusive_ptr.hpp>
#include <caf/ref_counted.hpp>

#include "broker/data.hh"
#include "broker/topic.hh"

#include "broker/detail/flare.hh"

namespace broker {
namespace detail {

/// Base class for `shared_publisher_queue` and `shared_subscriber_queue`.
class shared_queue : public caf::ref_counted {
public:
  using element_type = std::pair<topic, data>;

  using guard_type = std::unique_lock<std::mutex>;

  // --- accessors -------------------------------------------------------------

  inline int fd() const {
    return fx_.fd();
  }

  inline long pending() const {
    return pending_.load();
  }

  inline long rate() const {
    return rate_.load();
  }

  size_t buffer_size() const;

  // --- mutators --------------------------------------------------------------

  inline void pending(long x) {
    pending_ = x;
  }

  inline void rate(long x) {
    rate_ = x;
  }

  inline void wait_on_flare() {
    fx_.await_one();
  }

  bool wait_on_flare(caf::duration timeout);

  template <class T>
  bool wait_on_flare_abs(T abs_timeout) {
    return fx_.await_one(abs_timeout);
  }

protected:
  shared_queue();

  /// Guards access to `xs`.
  mutable std::mutex mtx_;

  /// Signals to users when data can be read or written.
  mutable flare fx_;

  /// Buffers values received by the worker.
  std::deque<element_type> xs_;

  /// Stores what demand the worker has last signaled to the core or vice
  /// versa, depending on the message direction.
  std::atomic<long> pending_;

  /// Stores consumption or production rate.
  std::atomic<size_t> rate_;
};

using shared_queue_ptr = caf::intrusive_ptr<shared_queue>;

shared_queue_ptr make_shared_queue();

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_SHARED_QUEUE_HH
