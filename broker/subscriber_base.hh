#ifndef BROKER_SUBSCRIBER_BASE_HH
#define BROKER_SUBSCRIBER_BASE_HH

#include <vector>

#include <caf/actor.hpp>
#include <caf/duration.hpp>
#include <caf/optional.hpp>
#include <caf/none.hpp>

#include "broker/data.hh"
#include "broker/fwd.hh"
#include "broker/topic.hh"

#include "broker/detail/assert.hh"
#include "broker/detail/shared_subscriber_queue.hh"

#ifdef CAF_LOG_COMPONENT
#undef CAF_LOG_COMPONENT
#endif

#define CAF_LOG_COMPONENT "broker"

namespace broker {

using duration = caf::duration;
using infinite_t = caf::infinite_t;
using caf::infinite;

inline duration to_duration(double secs) {
    return duration(std::chrono::milliseconds((int)(secs * 1e3)));
}

/// Provides blocking access to a stream of data.
template <class ValueType>
class subscriber_base {
public:
  // --- nested types ----------------------------------------------------------

  using value_type = ValueType;

  using queue_type = detail::shared_subscriber_queue<value_type>;

  using queue_ptr = detail::shared_subscriber_queue_ptr<value_type>;

  // --- constructors and destructors ------------------------------------------

  subscriber_base(long max_qsize)
    : queue_(detail::make_shared_subscriber_queue<value_type>()),
      max_qsize_(max_qsize) {
    // nop
  }

  subscriber_base(subscriber_base&&) = default;

  subscriber_base& operator=(subscriber_base&&) = default;

  virtual ~subscriber_base() {
    // nop
  }

  subscriber_base(const subscriber_base&) = delete;

  subscriber_base& operator=(const subscriber_base&) = delete;

  // --- access to values ------------------------------------------------------

  /// Pulls a single value out of the stream. Blocks the current thread until
  /// at least one value becomes available.
  value_type get() {
    auto tmp = get(1);
    BROKER_ASSERT(tmp.size() == 1);
    auto x = std::move(tmp.front());
    CAF_LOG_INFO("received" << x);
    return x;
  }

  /// Pulls a single value out of the stream. Blocks the current thread until
  /// at least one value becomes available or a timeout occurred.
  caf::optional<value_type> get(duration timeout) {
    auto tmp = get(1, timeout);
    if (tmp.size() == 1) {
      auto x = std::move(tmp.front());
      CAF_LOG_INFO("received" << x);
      return caf::optional<value_type>(std::move(x));
    }
    return caf::none;
  }

  /// Pulls `num` values out of the stream. Blocks the current thread until
  /// `num` elements are available or a timeout occurs. Returns a partially
  /// filled or empty vector on timeout, otherwise a vector containing exactly
  /// `num` elements.
  std::vector<value_type> get(size_t num,
                              duration timeout = infinite) {
    std::vector<value_type> result;
    if (num == 0)
      return result;
    auto t0 = std::chrono::high_resolution_clock::now();
    t0 += timeout;
    for (;;) {
      if (!timeout.valid())
        queue_->wait_on_flare();
      else if (!queue_->wait_on_flare_abs(t0))
        return result;
      size_t prev_size = 0;
      queue_->consume(num - result.size(), &prev_size, [&](value_type&& x) {
        CAF_LOG_INFO("received" << x);
        result.emplace_back(std::move(x));
      });
      if (prev_size >= static_cast<size_t>(max_qsize_))
        became_not_full();
      if (result.size() == num) {
        return result;
      }
    }
  }

  /// Returns all currently available values without blocking.
  std::vector<value_type> poll() {
    std::vector<value_type> result;
    result.reserve(queue_->buffer_size());
    size_t prev_size = 0;
    queue_->consume(std::numeric_limits<size_t>::max(), &prev_size,
                    [&](value_type&& x) { result.emplace_back(std::move(x)); });
    if (prev_size >= static_cast<size_t>(max_qsize_))
      became_not_full();
    return result;
  }

  // --- accessors -------------------------------------------------------------

  /// Returns the amound of values than can be extracted immediately without
  /// blocking.
  size_t available() const {
    return queue_->buffer_size();
  }

  /// Returns a file handle for integrating this publisher into a `select` or
  /// `poll` loop.
  int fd() const {
    return queue_->fd();
  }

protected:
  /// This hook allows subclasses to perform some action if the queue changed
  /// state from full to not-full. This allows subscribers to make sure new
  /// credit gets emitted in case no credit was available previously.
  virtual void became_not_full() {
    // nop
  }

  queue_ptr queue_;
  long max_qsize_;
};

} // namespace broker

#endif // BROKER_SUBSCRIBER_BASE_HH
