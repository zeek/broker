#ifndef BROKER_CONFIGURATION_HH
#define BROKER_CONFIGURATION_HH

#include <caf/actor_system_config.hpp>

namespace broker {

class context;

/// Provides an execution context for brokers.
class configuration {
  friend context;

public:
  /// Default-constructs a configuration.
  configuration();

  /// Constructs a configuration from the command line.
  configuration(int argc, char** argv);

private:
  caf::actor_system_config config_;
};

} // namespace broker

#endif // BROKER_CONFIGURATION_HH
