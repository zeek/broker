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
#include "broker/subscriber_base.hh"

#include "broker/detail/shared_subscriber_queue.hh"

namespace broker {

/// Provides blocking access to a stream of data.
class subscriber : public subscriber_base<std::pair<topic, data>> {
public:
  // --- friend declarations ---------------------------------------------------

  friend class endpoint;

  // --- constructors and destructors ------------------------------------------

  subscriber(subscriber&&) = default;

  subscriber& operator=(subscriber&&) = default;

  ~subscriber();

  size_t rate() const;

  inline const caf::actor& worker() const {
    return worker_;
  }

private:
  // -- force users to use `endpoint::make_event_subscriber` -------------------
  subscriber(endpoint& ep, std::vector<topic> ts, long max_qsize);

  caf::actor worker_;
};

} // namespace broker

#endif // BROKER_SUBSCRIBER_HH
