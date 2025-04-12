#pragma once

#include "broker/detail/native_socket.hh"
#include "broker/fwd.hh"
#include "broker/message.hh"
#include "broker/time.hh"

#include <chrono>
#include <memory>
#include <vector>

namespace broker::internal {

class hub_impl;

} // namespace broker::internal

namespace broker {

/// Hubs act as a subscriber and publisher at the same time.
class hub {
public:
  // --- static utility functions ----------------------------------------------

  static hub_id next_id() noexcept;

  // --- constructors, destructors, and assignment operators -------------------

  hub(hub&&) noexcept = default;

  hub& operator=(hub&&) noexcept = default;

  hub(const hub&) noexcept = delete;

  hub& operator=(const hub&) noexcept = delete;

  ~hub();

  static hub make(endpoint& ep, filter_type filter);

  // --- accessors -------------------------------------------------------------

  /// Returns the amount of values than can be extracted immediately without
  /// blocking.
  size_t available() const noexcept;

  /// Returns the current demand on this publisher. The demand is the amount of
  /// messages that were requested by the Broker core.
  size_t demand() const;

  /// Returns the current size of the output queue.
  size_t buffered() const;

  /// Returns the capacity of the output queue.
  size_t capacity() const;

  /// Returns a file handle for integrating this publisher into a `select` or
  /// `poll` loop. The socket descriptor becomes ready if at least one value is
  /// available to read.
  detail::native_socket read_fd() const noexcept;

  /// Returns a file handle for integrating this publisher into a `select` or
  /// `poll` loop. The socket descriptor becomes ready if the hub is ready to
  /// accept new values.
  detail::native_socket write_fd() const noexcept;

  // --- access to values ------------------------------------------------------

  /// Returns all currently available values without blocking.
  std::vector<data_message> poll();

  /// Pulls a single value out of the stream. Blocks the current thread until
  /// at least one value becomes available.
  data_message get();

  /// Returns `num` values, blocking the caller if necessary.
  std::vector<data_message> get(size_t num);

  /// Pulls a single value out of the stream. Blocks the current thread until
  /// at least one value becomes available or a timeout occurred.
  /// @return The message on success, `nullptr` on timeout.
  template <class Rep, class Period>
  data_message get(std::chrono::duration<Rep, Period> rel_timeout) {
    return do_get(std::chrono::duration_cast<timespan>(rel_timeout));
  }

  // --- topic management ------------------------------------------------------

  void subscribe(const topic& x, bool block = false);

  void unsubscribe(const topic& x, bool block = false);

  // --- messaging -------------------------------------------------------------

  /// Sends `x` to all subscribers.
  void publish(const topic& dest, set_builder&& content);

  /// Sends `x` to all subscribers.
  void publish(const topic& dest, table_builder&& content);

  /// Sends `x` to all subscribers.
  void publish(const topic& dest, list_builder&& content);

private:
  data_message do_get(timespan timeout);

  data_message do_get(timestamp timeout);

  explicit hub(std::shared_ptr<internal::hub_impl> ptr);

  std::shared_ptr<internal::hub_impl> impl_;
};

} // namespace broker
