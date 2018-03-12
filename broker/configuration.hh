#ifndef BROKER_CONFIGURATION_HH
#define BROKER_CONFIGURATION_HH

#include <caf/actor_system_config.hpp>

namespace broker {

struct broker_options {
  /// If true, peer connections won't use SSL.
  bool disable_ssl = false;
  /// If true, endpoints will forward incoming messages to peers.
  bool forward = true;
  /// TTL to insert into forwarded messages. Messages will be droppped once
  /// they have traversed more than this many hops. Note that the 1st
  /// receiver inserts the TTL (not the sender!). The 1st receiver does
  /// already count against the TTL.
  unsigned int ttl = 20;
  /// Whether you want to manually control the endpoint's flow of time.
  bool use_custom_clock = false;
  broker_options() {}
};

/// Provides an execution context for brokers.
class configuration : public caf::actor_system_config {
public:
  /// Default-constructs a configuration.
  configuration(broker_options opts = broker_options());

  /// Constructs a configuration from the command line.
  configuration(int argc, char** argv);

  const broker_options& options() const { return options_; }

private:
  const broker_options options_;
};

} // namespace broker

#endif // BROKER_CONFIGURATION_HH
