#pragma once

#include "broker/time.hh"

#include <memory>
#include <string>
#include <string_view>
#include <vector>

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
    /// The event carries information that might be relevant for a user to
    /// understand the system behavior.
    verbose,
    /// The event carries information relevant only for troubleshooting and
    /// debugging purposes.
    debug
  };

  /// Encodes the component that has emitted the event.
  enum class component_type : uint32_t {
    /// The Broker core has emitted the event.
    core = 0b000'0001,
    /// The Broker endpoint has emitted the event.
    endpoint = 0b000'0010,
    /// A Broker data store has emitted the event.
    store = 0b000'0100,
    /// The Broker network layer has emitted the event.
    network = 0b000'1000,
    /// A user-defined component has emitted the event.
    app = 0b001'0000,
  };

  enum class component_mask : uint32_t {};

  static constexpr auto nil_component_mask = static_cast<component_mask>(0);

  static constexpr auto default_component_mask =
    static_cast<component_mask>(0xFFFFFFFF);

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

constexpr event::component_mask operator|(event::component_type lhs,
                                          event::component_type rhs) noexcept {
  auto res = static_cast<uint32_t>(lhs) | static_cast<uint32_t>(rhs);
  return static_cast<event::component_mask>(res);
}

constexpr event::component_mask operator|(event::component_mask lhs,
                                          event::component_type rhs) noexcept {
  auto res = static_cast<uint32_t>(lhs) | static_cast<uint32_t>(rhs);
  return static_cast<event::component_mask>(res);
}

constexpr event::component_mask operator|(event::component_type lhs,
                                          event::component_mask rhs) noexcept {
  auto res = static_cast<uint32_t>(lhs) | static_cast<uint32_t>(rhs);
  return static_cast<event::component_mask>(res);
}

constexpr bool has_component(event::component_mask mask,
                             event::component_type component) noexcept {
  return (static_cast<uint32_t>(mask) & static_cast<uint32_t>(component)) != 0;
}

/// A smart pointer holding an immutable ::event.
using event_ptr = std::shared_ptr<const event>;

} // namespace broker
