#ifndef BROKER_CONFIGURATION_HH
#define BROKER_CONFIGURATION_HH

#include <caf/actor_system_config.hpp>

namespace broker {

/// Provides an execution context for brokers.
class configuration : public caf::actor_system_config {
public:
  /// Default-constructs a configuration.
  configuration(bool disable_ssl=false);

  /// Constructs a configuration from the command line.
  configuration(int argc, char** argv);

  /// Returns true if we use SSL for network connections. This is derived
  /// from the current configuration.
  bool using_ssl() const { return using_ssl_; }

private:
  const bool using_ssl_;
};

} // namespace broker

#endif // BROKER_CONFIGURATION_HH
