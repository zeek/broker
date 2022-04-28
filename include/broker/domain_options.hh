#pragma once

#include <caf/fwd.hpp>

namespace broker {

/// Bundles options for a Broker @ref gateway domain.
struct domain_options {
  /// If `true`, configures the gateway to appear only as a sink to other
  /// peers.
  bool disable_forwarding = false;

  /// Stores all options to `sink`.
  void save(caf::settings& sink);

  /// Loads all options from `source`.
  void load(const caf::settings& source);
};

} // namespace broker
