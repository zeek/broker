#pragma once

#include "broker/configuration.hh"
#include "broker/fwd.hh"
#include "broker/internal/fwd.hh"

#include <caf/actor_system.hpp>
#include <caf/fwd.hpp>

#include <memory>

namespace broker::internal {

struct endpoint_context {
  configuration cfg;
  caf::actor_system sys;
  explicit endpoint_context(configuration&& src);
};

using endpoint_context_ptr = std::shared_ptr<internal::endpoint_context>;

struct endpoint_access {
  explicit endpoint_access(endpoint* ep) : ep(ep) {
    // nop
  }

  caf::actor_system& sys();

  const caf::actor_system_config& cfg();

  endpoint_context_ptr ctx();

  endpoint* ep;
};

} // namespace broker::internal

// Note: member functions of this utility need access to types only visible in
//       endpoint.cc and they are therefore implemented in the same file.
