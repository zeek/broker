#pragma once

#include "broker/detail/native_socket.hh"
#include "broker/detail/opaque_type.hh"
#include "broker/fwd.hh"
#include "broker/message.hh"

#include <cstddef>
#include <mutex>
#include <vector>

namespace broker::internal {

class hub_impl;

} // namespace broker::internal

namespace broker {

/// Provides asynchronous publishing of data with demand management.
class publisher {
public:
  // --- friend declarations ---------------------------------------------------

  friend class endpoint;

  // --- nested types ----------------------------------------------------------

  using value_type = data_message;

  using guard_type = std::unique_lock<std::mutex>;

  // --- constructors and destructors ------------------------------------------

  publisher(publisher&&) noexcept = default;

  publisher& operator=(publisher&&) noexcept = default;

  publisher(const publisher&) = delete;

  publisher& operator=(const publisher&) = delete;

  ~publisher();

  // --- factories -------------------------------------------------------------

  static publisher make(endpoint& ep, topic t);

  // --- accessors -------------------------------------------------------------

  /// Returns the current demand on this publisher. The demand is the amount of
  /// messages that were requested by the Broker core.
  size_t demand() const;

  /// Returns the current size of the output queue.
  size_t buffered() const;

  /// Returns the capacity of the output queue.
  size_t capacity() const;

  /// Returns the free capacity of the output queue, i.e., how many items can
  /// be enqueued before it starts blocking.
  size_t free_capacity() const;

  /// Returns a file handle for integrating this publisher into a `select` or
  /// `poll` loop.
  detail::native_socket fd() const;

  // --- mutators --------------------------------------------------------------

  /// @deprecated No longer has any effect.
  void drop_all_on_destruction();

  // --- messaging -------------------------------------------------------------

  /// Sends `x` to all subscribers.
  void publish(const data& val);

  /// Sends `xs` to all subscribers.
  void publish(const std::vector<data>& vals);

  /// Sends `x` to all subscribers.
  void publish(set_builder&& content);

  /// Sends `x` to all subscribers.
  void publish(table_builder&& content);

  /// Sends `x` to all subscribers.
  void publish(list_builder&& content);

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
  // Private to force users to use `endpoint::make_publsiher`.
  // Note: takes ownership of `q` (not increasing ref count!).
  publisher(topic dst, std::shared_ptr<internal::hub_impl> impl);

  topic dst_;
  std::shared_ptr<internal::hub_impl> impl_;
};

} // namespace broker
