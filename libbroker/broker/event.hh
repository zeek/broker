#pragma once

#include "broker/time.hh"

#include <memory>
#include <string>
#include <string_view>

namespace broker {

/// Carries information about internal events to the user that are crucial for
/// understanding the system behavior.
class event {
public:
  /// Encodes the severity of the emitted event.
  enum class severity_level {
    /// The reported event is most likely fatal. After a critical event, normal
    /// operation has most likely broken down.
    critical,
    /// The event signals an unrecoverable error in Broker. For example, this
    /// might signal a broken network connection or a data store that is no
    /// longer functional after losing connection to the master node.
    error,
    /// The event signals an unexpected or conspicuous system state that may
    /// still be recoverable.
    warning,
    /// Signals a noteworthy event during normal system operation such as a new
    /// peering connection.
    info,
    /// The event carries information relevant only for troubleshooting and
    /// debugging purposes.
    debug
  };

  /// Encodes the component that has emitted the event.
  enum class component_type {
    /// The Broker core has emitted the event.
    core,
    /// The endpoint has emitted the event.
    endpoint,
    /// A Broker data store running in master mode has emitted the event.
    master_store,
    /// A Broker data store running in clone mode has emitted the event.
    clone_store,
    /// The Broker network layer has emitted the event.
    network,
    /// A Broker backend has emitted the event. This could be a SQLite database
    /// or an in-memory store.
    store_backend,
  };

  /// The time when the event has been emitted.
  broker::timestamp timestamp;

  /// Stores the severity for this event.
  severity_level severity;

  /// Stores which Broker component has emitted this event.
  component_type component;

  /// A unique identifier for the event.
  std::string_view identifier;

  /// A human-readable description of the logged event.
  std::string description;

  event(severity_level severity, component_type component,
        std::string_view identifier, std::string description)
    : timestamp(broker::now()),
      severity(severity),
      component(component),
      identifier(identifier),
      description(std::move(description)) {}
};

/// A smart pointer holding an immutable ::event.
using event_ptr = std::shared_ptr<const event>;

} // namespace broker
