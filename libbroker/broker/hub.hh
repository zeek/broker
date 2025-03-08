#pragma once

#include "broker/detail/native_socket.hh"
#include "broker/fwd.hh"

#include <memory>
#include <vector>

namespace broker {

/// Strong type alias for uniquely identifying hubs.
enum class hub_id : uint64_t {
  /// Represents an invalid hub ID.
  invalid = 0,
};

/// Hubs act as a subscriber and publisher at the same time.
class hub {
public:
  ~hub();

  static hub_id next_id() noexcept;

  // --- access to values ------------------------------------------------------

  /// Returns all currently available values without blocking.
  std::vector<data_message> poll();

  /// Pulls a single value out of the stream. Blocks the current thread until
  /// at least one value becomes available.
  data_message get();

  /// Returns `num` values, blocking the caller if necessary.
  std::vector<data_message> get(size_t num);

  // --- accessors -------------------------------------------------------------

  /// Returns the amount of values than can be extracted immediately without
  /// blocking.
  size_t available() const noexcept;

  /// Returns a file handle for integrating this publisher into a `select` or
  /// `poll` loop. The socket descriptor becomes ready if at least one value is
  /// available to read.
  detail::native_socket read_fd() const noexcept;

  /// Returns a file handle for integrating this publisher into a `select` or
  /// `poll` loop. The socket descriptor becomes ready if the hub is ready to
  /// accept new values.
  detail::native_socket write_fd() const noexcept;

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
  class impl;

  std::shared_ptr<impl> impl_;
};

} // namespace broker
