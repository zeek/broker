#pragma once

#include <memory>

namespace broker {

/// An interface for observing internal events in Broker.
class event_observer {
public:
  virtual ~event_observer();
};

/// A smart pointer holding an ::event_observer.
using event_observer_ptr = std::shared_ptr<event_observer>;

} // namespace broker
