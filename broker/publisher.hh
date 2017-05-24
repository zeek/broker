#ifndef BROKER_PUBLISHER_HH
#define BROKER_PUBLISHER_HH

#include <chrono>
#include <cstddef>
#include <vector>

#include <caf/actor.hpp>
#include <caf/duration.hpp>

#include "broker/atoms.hh"
#include "broker/fwd.hh"

#include "broker/detail/shared_publisher_queue.hh"

namespace broker {

/// Provides asynchronous publishing of data with demand management.
class publisher {
public:
  // --- friend declarations ---------------------------------------------------

  friend class endpoint;

  // --- nested types ----------------------------------------------------------

  using value_type = std::pair<topic, data>;

  using guard_type = std::unique_lock<std::mutex>;

  // --- constructors and destructors ------------------------------------------

  publisher(publisher&&) = default;

  publisher& operator=(publisher&&) = default;

  publisher(const publisher&) = delete;

  publisher& operator=(const publisher&) = delete;

  ~publisher();

  // --- accessors -------------------------------------------------------------

  /// Returns the current demand on this publisher. The demand is the amount of
  /// messages that can send to the core immediately plus a small desired
  /// buffer size to minimize latency (usually 5 extra items).
  size_t demand() const;

  /// Returns the current size of the output queue.
  size_t buffered() const;

  /// Returns a rough estimate of the throughput per second of this publisher.
  size_t send_rate() const;
  
  /// Returns a reference to the background worker.
  inline const caf::actor& worker() const {
    return worker_;
  }

  /// Returns a file handle for integrating this publisher into a `select` or
  /// `poll` loop.
  inline int fd() const {
    return queue_->fd();
  }

  // --- messaging -------------------------------------------------------------
  
  /// Sends `x` to all subscribers.
  void publish(data x);

  /// Sends `xs` to all subscribers.
  void publish(std::vector<data> xs);

  /// Blocks the current thread until a timeout occurs or until the publisher
  /// has sufficient capacity to send `min_demand` messages immediately.
  /// Returns `false` if a timeout occured, `true` otherwise.
  /// @warning Picking a high value for `min_demand` can result in a deadlock
  ///          when setting `timeout` to `infinite`. Demand is assigned to
  ///          publishers according to the credit policy of the core. The core
  ///          is free to assign only small capacity kvalues to each publisher
  ///          individually in order to balance fairness and overall
  ///          throughput.
  bool wait_for_demand(caf::duration timeout = caf::infinite);

private:
  // -- force users to use `endpoint::make_publsiher` -------------------------
  publisher(endpoint& ep, topic t);

  detail::shared_publisher_queue_ptr<> queue_;
  caf::actor worker_;
  topic topic_;
};

} // namespace broker

#endif // BROKER_PUBLISHER_HH
