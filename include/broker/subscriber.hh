#pragma once

#include "broker/data.hh"
#include "broker/detail/native_socket.hh"
#include "broker/detail/opaque_type.hh"
#include "broker/fwd.hh"
#include "broker/message.hh"
#include "broker/topic.hh"
#include "broker/worker.hh"

#include <functional>
#include <vector>

namespace broker {

/// Provides blocking access to a stream of data.
class subscriber {
public:
  // --- friend declarations ---------------------------------------------------

  friend class endpoint;

  friend class status_subscriber;

  // -- member types -----------------------------------------------------------

  using optional_data_message = std::optional<data_message>;

  // --- constructors and destructors ------------------------------------------

  subscriber(subscriber&&) = default;

  subscriber& operator=(subscriber&&) = default;

  subscriber(const subscriber&) = delete;

  subscriber& operator=(const subscriber&) = delete;

  ~subscriber();

  // --- factories -------------------------------------------------------------

  static subscriber make(endpoint& ep, filter_type filter, size_t queue_size);

  // --- access to values ------------------------------------------------------

  /// Returns all currently available values without blocking.
  std::vector<data_message> poll();

  /// Pulls a single value out of the stream. Blocks the current thread until
  /// at least one value becomes available.
  data_message get();

  /// Returns `num` values, blocking the caller if necessary.
  std::vector<data_message> get(size_t num);

  /// Pulls `num` values out of the stream. Blocks the current thread until
  /// `num` elements are available or a timeout occurs. Returns a partially
  /// filled or empty vector on timeout, otherwise a vector containing exactly
  /// `num` elements.
  template <class Duration>
  std::vector<data_message>
  get(size_t num, std::chrono::time_point<clock, Duration> abs_timeout) {
    using std::chrono::time_point_cast;
    return do_get(num, time_point_cast<timespan>(abs_timeout));
  }

  /// Pulls `num` values out of the stream. Blocks the current thread until
  /// `num` elements are available or a timeout occurs. Returns a partially
  /// filled or empty vector on timeout, otherwise a vector containing exactly
  /// `num` elements.
  template <class Rep, class Period>
  std::vector<data_message>
  get(size_t num, std::chrono::duration<Rep, Period> rel_timeout) {
    if (rel_timeout != infinite)
      return do_get(num, now() + rel_timeout);
    else
      return get(num);
  }

  /// Pulls a single value out of the stream. Blocks the current thread until
  /// at least one value becomes available or a timeout occurred.
  template <class Duration>
  optional_data_message
  get(std::chrono::time_point<clock, Duration> abs_timeout) {
    optional_data_message result;
    auto tmp = get(1, abs_timeout);
    if (tmp.size() == 1)
      result.emplace(std::move(tmp.front()));
    return result;
  }

  /// Pulls a single value out of the stream. Blocks the current thread until
  /// at least one value becomes available or a timeout occurred.
  template <class Rep, class Period>
  optional_data_message get(std::chrono::duration<Rep, Period> rel_timeout) {
    optional_data_message result;
    auto tmp = get(1, rel_timeout);
    if (tmp.size() == 1)
      result.emplace(std::move(tmp.front()));
    return result;
  }

  // --- accessors -------------------------------------------------------------

  /// Returns the amount of values than can be extracted immediately without
  /// blocking.
  size_t available() const noexcept;

  /// Returns a file handle for integrating this publisher into a `select` or
  /// `poll` loop.
  detail::native_socket fd() const noexcept;

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
  subscriber(detail::opaque_ptr queue, std::shared_ptr<filter_type> core_filter,
             worker core);

  void update_filter(topic x, bool add, bool block);

  std::vector<data_message> do_get(size_t num, timestamp abs_timeout);

  void do_get(std::vector<data_message>& buf, size_t num,
              timestamp abs_timeout);

  void wait();

  bool wait_for(timespan);

  bool wait_until(timestamp);

  /// Points to the actual implementation for the producer-consumer queue.
  detail::opaque_ptr queue_;

  /// Holds onto a reference to the core actor for filter updates.
  worker core_;

  /// Points to the filter held by the core actor. May only be touched in the
  /// context of the core.
  std::shared_ptr<filter_type> core_filter_;
};

} // namespace broker
