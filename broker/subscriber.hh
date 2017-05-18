#ifndef BROKER_SUBSCRIBER_HH
#define BROKER_SUBSCRIBER_HH

#include <vector>

#include <caf/actor.hpp>
#include <caf/duration.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/ref_counted.hpp>

#include "broker/data.hh"
#include "broker/fwd.hh"
#include "broker/topic.hh"

#include "broker/detail/shared_queue.hh"

namespace broker {

/// Provides blocking access to a stream of data.
class subscriber {
public:
  // --- nested types ----------------------------------------------------------
  
  using value_type = std::pair<topic, data>;

  using guard_type = std::unique_lock<std::mutex>;

  // --- constructors and destructors ------------------------------------------

  subscriber(endpoint& ep, std::vector<topic> ts, long max_qsize = 20);

  ~subscriber();

  /// Pulls a single value out of the stream. Blocks the current thread until
  /// at least one value becomes available.
  value_type get();

  /// Pulls a single value out of the stream. Blocks the current thread until
  /// at least one value becomes available or a timeout occurred.
  caf::optional<value_type> get(caf::duration timeout);

  /// Pulls `num` values out of the stream. Blocks the current thread until
  /// `num` elements are available or a timeout occurs. Returns a partially
  /// filled or empty vector on timeout, otherwise a vector containing exactly
  /// `num` elements.
  std::vector<value_type> get(size_t num, caf::duration = caf::infinite);
  
  /// Returns all currently available values without blocking.
  std::vector<value_type> poll();
  
  /// Returns the amound of values than can be extracted immediately without
  /// blocking.
  size_t available() const;

  /// Returns the worker serving this subscriber.
  const caf::actor& worker() const {
    return worker_;
  }

private:
  detail::shared_queue_ptr queue_;
  caf::actor worker_;
};

} // namespace broker

#endif // BROKER_SUBSCRIBER_HH
