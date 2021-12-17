#pragma once

#include "broker/fwd.hh"

#include <caf/fwd.hpp>

namespace broker::internal {

struct endpoint_access {
  explicit endpoint_access(endpoint* ep) : ep(ep) {
    // nop
  }

  caf::actor_system& sys();

  const caf::actor_system_config& cfg();

  endpoint* ep;
};

} // namespace broker::internal

// Note: member functions of this utility need access to types only visible in
//       endpoint.cc and they are therefore implemented in the same file.
