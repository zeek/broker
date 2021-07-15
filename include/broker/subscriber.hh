#pragma once

#include <vector>

#include <caf/none.hpp>
#include <caf/optional.hpp>

#include "broker/data.hh"
#include "broker/detail/shared_subscriber_queue.hh"
#include "broker/filter_type.hh"
#include "broker/fwd.hh"
#include "broker/message.hh"
#include "broker/topic.hh"

namespace broker {

/// Provides blocking access to a stream of data.
class subscriber {
public:
  // --- friend declarations ---------------------------------------------------

  friend class endpoint;

  friend class status_subscriber;

  // --- nested types ----------------------------------------------------------

  // --- nested types ----------------------------------------------------------

  using queue_type = detail::shared_subscriber_queue<data_message>;

  using queue_ptr = caf::intrusive_ptr<queue_type>;

  // --- constructors and destructors ------------------------------------------

  subscriber(subscriber&&) = default;

  subscriber& operator=(subscriber&&) = default;

  subscriber(const subscriber&) = delete;

  subscriber& operator=(const subscriber&) = delete;

  ~subscriber();

  // --- factories -------------------------------------------------------------

  static subscriber make(endpoint& ep, filter_type filter, size_t queue_size);

  // --- access to values ------------------------------------------------------

  /// Pulls a single value out of the stream. Blocks the current thread until
  /// at least one value becomes available.
  data_message get();

  /// Pulls a single value out of the stream. Blocks the current thread until
  /// at least one value becomes available or a timeout occurred.
  template <class Timeout,
            class = std::enable_if_t<!std::is_integral<Timeout>::value>>
  caf::optional<data_message> get(Timeout timeout) {
    auto tmp = get(1, timeout);
    if (tmp.size() == 1) {
      auto x = std::move(tmp.front());
      BROKER_DEBUG("received" << x);
      return caf::optional<data_message>(std::move(x));
    }
    return caf::none;
  }

  /// Pulls `num` values out of the stream. Blocks the current thread until
  /// `num` elements are available or a timeout occurs. Returns a partially
  /// filled or empty vector on timeout, otherwise a vector containing exactly
  /// `num` elements.
  template <class Clock, class Duration>
  std::vector<data_message>
  get(size_t num, std::chrono::time_point<Clock, Duration> timeout) {
    std::vector<data_message> result;
    if (num == 0)
      return result;
    if (timeout <= std::chrono::system_clock::now())
      return result;
    result.reserve(num);
    for (;;) {
      if (!queue_->wait_on_flare_abs(timeout))
        return result;
      size_t prev_size = 0;
      auto remaining = num - result.size();
      auto got = queue_->consume(remaining, &prev_size, [&](data_message&& x) {
        BROKER_DEBUG("received" << x);
        result.emplace_back(std::move(x));
      });
      if (result.size() == num)
        return result;
    }
  }

  /// Pulls `num` values out of the stream. Blocks the current thread until
  /// `num` elements are available or a timeout occurs. Returns a partially
  /// filled or empty vector on timeout, otherwise a vector containing exactly
  /// `num` elements.
  template <class Duration>
  std::vector<data_message> get(size_t num, Duration relative_timeout) {
    if (!caf::is_infinite(relative_timeout)) {
      auto timeout = caf::make_timestamp();
      timeout += relative_timeout;
      return get(num, timeout);
    }
    std::vector<data_message> result;
    if (num == 0)
      return result;
    result.reserve(num);
    for (;;) {
      queue_->wait_on_flare();
      size_t prev_size = 0;
      auto remaining = num - result.size();
      auto got = queue_->consume(remaining, &prev_size, [&](data_message&& x) {
        BROKER_DEBUG("received" << x);
        result.emplace_back(std::move(x));
      });
      if (result.size() == num)
        return result;
    }
  }

  /// Returns `num` values, blocking the caller if necessary.
  std::vector<data_message> get(size_t num);

  /// Returns all currently available values without blocking.
  std::vector<data_message> poll();

  // --- accessors -------------------------------------------------------------

  /// Returns the amount of values than can be extracted immediately without
  /// blocking.
  size_t available() const noexcept {
    return queue_->buffer_size();
  }

  /// Returns a file handle for integrating this publisher into a `select` or
  /// `poll` loop.
  int fd() const noexcept {
    return static_cast<int>(queue_->fd());
  }

  // --- topic management ------------------------------------------------------

  void add_topic(topic x, bool block = false);

  void remove_topic(topic x, bool block = false);

  // --- miscellaneous ---------------------------------------------------------

  /// Release any state held by the object, rendering it invalid.
  /// @warning Performing *any* action on this object afterwards invokes
  ///          undefined behavior, except:
  ///          - Destroying the object by calling the destructor.
  ///          - Using copy- or move-assign from a valid `store` to "revive"
  ///            this object.
  ///          - Calling `reset` again (multiple invocations are no-ops).
  /// @note This member function specifically targets the Python bindings. When
  ///       writing Broker applications using the native C++ API, there's no
  ///       point in calling this member function.
  void reset();

private:
  subscriber(queue_ptr queue, filter_type filter, caf::actor core);

  void update_filter(bool block);

  queue_ptr queue_;

  filter_type filter_;

  caf::actor core_;
};

} // namespace broker
