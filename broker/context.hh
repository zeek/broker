#ifndef BROKER_CONTEXT_HH
#define BROKER_CONTEXT_HH

#include <caf/actor_system.hpp>

#include "broker/detail/type_traits.hh"

#include "broker/api_flags.hh"
#include "broker/configuration.hh"
#include "broker/endpoint.hh"
#include "broker/fwd.hh"

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
  template <api_flags Flags>
  detail::enable_if_t<has_api_flags(Flags, blocking), blocking_endpoint>
  spawn() {
    return {system_, Flags};
  }

  /// Creates a ::nonblocking_endpoint.
  template <api_flags Flags, class... Ts>
  detail::enable_if_t<has_api_flags(Flags, nonblocking), nonblocking_endpoint>
  spawn() {
    return {system_, Flags};
  }

private:
  configuration config_;
  caf::actor_system system_;
};

} // namespace broker

#endif // BROKER_CONTEXT_HH
