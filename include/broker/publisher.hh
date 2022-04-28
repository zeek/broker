#pragma once

#include "broker/detail/native_socket.hh"
#include "broker/detail/opaque_type.hh"
#include "broker/entity_id.hh"
#include "broker/fwd.hh"
#include "broker/message.hh"

#include <chrono>
#include <cstddef>
#include <vector>

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

  publisher(publisher&&) = default;

  publisher& operator=(publisher&&) = default;

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

  /// Forces the publisher to drop all remaining items from the queue when the
  /// destructor gets called.
  void drop_all_on_destruction();

  // --- messaging -------------------------------------------------------------

  /// Sends `x` to all subscribers.
  void publish(data x);

  /// Sends `xs` to all subscribers.
  void publish(std::vector<data> xs);

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
  // -- force users to use `endpoint::make_publsiher` -------------------------
  publisher(detail::opaque_ptr q, topic t);

  detail::opaque_ptr queue_;
  topic topic_;
  bool drop_on_destruction_ = false;
};

} // namespace broker
