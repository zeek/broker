#ifndef BROKER_EVENT_SUBSCRIBER_HH
#define BROKER_EVENT_SUBSCRIBER_HH

#include <vector>

#include <caf/actor.hpp>
#include <caf/variant.hpp>

#include "broker/error.hh"
#include "broker/fwd.hh"
#include "broker/status.hh"
#include "broker/subscriber_base.hh"

#include "broker/detail/shared_subscriber_queue.hh"

namespace broker {

/// Provides blocking access to a stream of endpoint events.
class status_subscriber
  : public subscriber_base<caf::variant<none, error, status>> {
public:
  // --- friend declarations ---------------------------------------------------

  friend class endpoint;

  // --- nested types ----------------------------------------------------------

  using super = subscriber_base<caf::variant<none, error, status>>;

  // --- constructors and destructors ------------------------------------------

  status_subscriber(status_subscriber&&) = default;

  status_subscriber& operator=(status_subscriber&&) = default;

  ~status_subscriber();

  inline const caf::actor& worker() const {
    return worker_;
  }

private:
  // -- force users to use `endpoint::make_status_subscriber` -------------------
  status_subscriber(endpoint& ep, bool receive_statuses = false);

  caf::actor worker_;
};

} // namespace broker

#endif // BROKER_EVENT_SUBSCRIBER_HH
