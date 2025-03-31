#pragma once

#include "broker/event.hh"

#include <memory>

namespace broker {

/// An interface for observing internal events in Broker.
class event_observer {
public:
  virtual ~event_observer();

  /// Called by Broker to notify the observer about a new event.
  /// @param what The event that Broker has emitted.
  /// @note This member function is called from multiple threads and thus must
  ///       be thread-safe.
  virtual void observe(event_ptr what) = 0;

  /// Returns true if the observer is interested in events of the given severity
  /// and component type. Returning false will cause Broker to not generate
  /// filtered events.
  virtual bool accepts(event::severity_level severity,
                       event::component_type component) const = 0;
};

/// A smart pointer holding an ::event_observer.
using event_observer_ptr = std::shared_ptr<event_observer>;

} // namespace broker
