#pragma once

#include <caf/fwd.hpp>

namespace broker {

/// Bundles options for a Broker @ref gateway domain.
struct domain_options {
  /// If `true`, configures the gateway to appear only as a sink to other
  /// peers.
  bool disable_forwarding = false;

  /// @relates domain_options
  void save(caf::settings& sink);

  /// @relates domain_options
  void load(const caf::settings& source);

};

} // namespace broker
