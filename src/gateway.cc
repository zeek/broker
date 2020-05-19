#include "broker/gateway.hh"

#include <caf/io/publish.hpp>
#include <caf/openssl/publish.hpp>

#include "broker/configuration.hh"
#include "broker/core_actor.hh"

namespace broker {

struct gateway::impl {
  // -- constructors, destructors, and assignment operators --------------------

  impl() : sys(cfg) {
    external = sys.spawn(core_actor, filter_type{}, nullptr);
    internal = sys.spawn(core_actor, filter_type{}, nullptr);
  }

  // -- member variables -------------------------------------------------------

  configuration cfg;
  caf::actor_system sys;
  caf::actor external;
  caf::actor internal;
};

void gateway::domain_options::disable_forwarding() {
  caf::put(settings_, "broker.disable-forwarding", true);
}

  // -- constructors, destructors, and assignment operators --------------------

gateway::~gateway() {
  // Must appear out-of-line because of ptr_.
}

gateway::gateway(std::unique_ptr<impl>&& ptr) : ptr_(std::move(ptr)) {
  // nop
}

expected<gateway> gateway::make() {
  return gateway{std::make_unique<impl>()};
}

// --- peer management ---------------------------------------------------------

uint16_t gateway::listen_external(const std::string& address, uint16_t port) {
  return listen_impl(ptr_->external, address, port);
}

uint16_t gateway::listen_internal(const std::string& address, uint16_t port) {
  return listen_impl(ptr_->internal, address, port);
}

uint16_t gateway::listen_impl(const caf::actor& core,
                              const std::string& address, uint16_t port) {
  char const* addr = address.empty() ? nullptr : address.c_str();
  auto publish = ptr_->cfg.options().disable_ssl
                 ? caf::io::publish<caf::actor>
                 : caf::openssl::publish<caf::actor>;
  if (auto res = publish(core, port, addr, true))
    return *res;
  return 0;
}

} // namespace broker
