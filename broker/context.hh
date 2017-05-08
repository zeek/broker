#ifndef BROKER_CONTEXT_HH
#define BROKER_CONTEXT_HH

#include <caf/actor_system.hpp>

#include "broker/detail/type_traits.hh"

#include "broker/api_flags.hh"
#include "broker/configuration.hh"
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

  /// Creates an ::endpoint.
  endpoint spawn();

  inline caf::actor_system& system() {
    return system_;
  }

  inline const caf::actor& core() const {
    return core_;
  }

private:
  configuration config_;
  caf::actor_system system_;
  caf::actor core_;
};

} // namespace broker

#endif // BROKER_CONTEXT_HH
