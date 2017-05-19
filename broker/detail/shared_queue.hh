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

namespace broker {
namespace detail {

/// Synchronizes a subscriber with a background worker.
struct shared_queue : caf::ref_counted {
  using element_type = std::pair<topic, data>;

  shared_queue();

  /// Guards access to all members of the queue.
  mutable std::mutex mtx;

  /// Allows the subscriber to wait for arriving data.
  std::condition_variable cv;

  /// Buffers values received by the worker.
  std::deque<element_type> xs;

  /// Stores what demand the worker has last signaled to the core or vice
  /// versa, depending on the message direction.
  std::atomic<long> pending;

  /// Stores consumption or production rate.
  std::atomic<size_t> rate;
};

using shared_queue_ptr = caf::intrusive_ptr<shared_queue>;

shared_queue_ptr make_shared_queue();

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_SHARED_QUEUE_HH
