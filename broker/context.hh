#ifndef BROKER_CONTEXT_HH
#define BROKER_CONTEXT_HH

#include <cstdint>

#include <caf/actor_system.hpp>
#include <caf/behavior.hpp>

#include "broker/detail/type_traits.hh"

#include "broker/configuration.hh"
#include "broker/endpoint.hh"
#include "broker/fwd.hh"
#include "broker/spawn_flags.hh"

namespace broker {

/// Provides an execution context for endpoints.
class context {
  // Access to actor system
  friend blocking_endpoint;
  friend nonblocking_endpoint;

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
    return {system_, caf::behavior{std::forward<Ts>(xs)...}};
  }

private:
  caf::actor_system system_;
};

} // namespace broker

#endif // BROKER_CONTEXT_HH
