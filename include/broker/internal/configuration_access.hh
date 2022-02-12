#pragma once

#include "broker/fwd.hh"

#include <caf/fwd.hpp>

namespace broker::internal {

struct configuration_access {
  explicit configuration_access(configuration* ptr) : ptr(ptr) {
    // nop
  }

  caf::actor_system_config& cfg();

  configuration* ptr;
};

} // namespace broker::internal

// Note: member functions of this utility need access to types only visible in
//       configuration.cc and they are therefore implemented in the same file.
