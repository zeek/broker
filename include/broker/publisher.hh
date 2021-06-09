#pragma once

#include <chrono>
#include <cstddef>
#include <vector>

#include <caf/actor.hpp>

#include "broker/atoms.hh"
#include "broker/entity_id.hh"
#include "broker/fwd.hh"
#include "broker/message.hh"

#include "broker/detail/shared_publisher_queue.hh"

namespace broker {

/// Provides asynchronous publishing of data with demand management.
class publisher {
public:
  // --- friend declarations ---------------------------------------------------

  friend class endpoint;

  // --- nested types ----------------------------------------------------------

  using value_type = data_message;

  using guard_type = std::unique_lock<std::mutex>;

  using queue_type = detail::shared_publisher_queue<data_message>;

  using queue_ptr = caf::intrusive_ptr<queue_type>;

  // --- constructors and destructors ------------------------------------------

  publisher(publisher&&) = default;

  publisher& operator=(publisher&&) = default;

  publisher(const publisher&) = delete;

  publisher& operator=(const publisher&) = delete;

  ~publisher();

  // --- factories -------------------------------------------------------------

  static publisher make(endpoint& ep, topic t);

  static publisher make(caf::actor sink, topic t);

  // --- accessors -------------------------------------------------------------

  /// Returns the current size of the output queue.
  size_t buffered() const;

  /// Returns the capacity of the output queue.
  size_t capacity() const;

  /// Returns the free capacity of the output queue, i.e., how many items can
  /// be enqueued before it starts blocking.
  size_t free_capacity() const;

  /// Returns a file handle for integrating this publisher into a `select` or
  /// `poll` loop.
  auto fd() const {
    return queue_->fd();
  }

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
  publisher(queue_ptr q, topic t);

  queue_ptr queue_;
  topic topic_;
  bool drop_on_destruction_ = false;
};

using publisher_id [[deprecated("use entity_id instead")]] = entity_id;

} // namespace broker
