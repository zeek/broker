#ifndef BROKER_EVENT_SUBSCRIBER_HH
#define BROKER_EVENT_SUBSCRIBER_HH

#include <vector>

#include <caf/actor.hpp>
#include <caf/duration.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/ref_counted.hpp>

#include "broker/error.hh"
#include "broker/fwd.hh"
#include "broker/status.hh"
#include "broker/subscriber_base.hh"

#include "broker/detail/shared_subscriber_queue.hh"
#include "broker/detail/variant.hh"

namespace broker {

/// Provides blocking access to a stream of endpoint events.
class event_subscriber
  : public subscriber_base<detail::variant<none, error, status>> {
public:
  // --- friend declarations ---------------------------------------------------

  friend class endpoint;

  // --- constructors and destructors ------------------------------------------

  event_subscriber(event_subscriber&&) = default;

  event_subscriber& operator=(event_subscriber&&) = default;

  ~event_subscriber();

  inline const caf::actor& worker() const {
    return worker_;
  }

private:
  // -- force users to use `endpoint::make_event_subscriber` -------------------
  event_subscriber(endpoint& ep, bool receive_statuses = false);

  caf::actor worker_;
};

inline std::string _debug_to_string(detail::variant<none, error, status> x) {
  return std::string("XXX");
}

} // namespace broker

#endif // BROKER_EVENT_SUBSCRIBER_HH
