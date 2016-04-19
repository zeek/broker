#ifndef BROKER_CONTEXT_HH
#define BROKER_CONTEXT_HH

#include <cstdint>

#include <caf/actor_system.hpp>
#include <caf/event_based_actor.hpp>

#include "broker/detail/type_traits.hh"

#include "broker/configuration.hh"
#include "broker/endpoint.hh"
#include "broker/spawn_flags.hh"

namespace broker {

class endpoint;
class blocking_endpoint;
class nonblocking_endpoint;

/// Provides an execution context for endpoints.
class context {
public:
  /// Constructs a context from a specific configuration.
  context(configuration config = {});

  /// Creates a ::blocking_endpoint.
  template <spawn_flags Flags>
  detail::enable_if_t<Flags == blocking, blocking_endpoint>
  spawn() {
    return {system_};
  }

  /// Creates a ::nonblocking_endpoint.
  template <spawn_flags Flags, class... Ts>
  detail::enable_if_t<Flags == nonblocking, nonblocking_endpoint>
  spawn(Ts&&... xs) {
    auto subscriber = system_.spawn([=] { return caf::behavior{xs...}; });
    return {system_, std::move(subscriber)};
  }

private:
  caf::actor_system system_;
};

} // namespace broker

#endif // BROKER_CONTEXT_HH
