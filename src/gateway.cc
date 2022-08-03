#include "broker/gateway.hh"

#include <caf/io/publish.hpp>
#include <caf/openssl/publish.hpp>
#include <caf/scoped_actor.hpp>

#include "broker/configuration.hh"
#include "broker/core_actor.hh"

namespace broker {

// -- member types -------------------------------------------------------------

struct gateway::impl {
  // -- constructors, destructors, and assignment operators --------------------

  impl(configuration&& source_config,
       const domain_options* adapt_internal = nullptr,
       const domain_options* adapt_external = nullptr)
    : cfg(std::move(source_config)), sys(cfg) {
    // Spin up two cores.
    alm_id = endpoint_id::random();
    internal = sys.spawn<core_actor>(alm_id, filter_type{}, nullptr,
                                     adapt_internal);
    external = sys.spawn<core_actor>(alm_id, filter_type{}, nullptr,
                                     adapt_external);
    gateway::setup(internal, external);
  }

  // -- member variables -------------------------------------------------------

  configuration cfg;
  caf::actor_system sys;
  endpoint_id alm_id;
  caf::actor internal;
  caf::actor external;
};

// -- constructors, destructors, and assignment operators ----------------------

gateway::~gateway() {
  // Must appear out-of-line because of ptr_.
}

gateway::gateway(std::unique_ptr<impl>&& ptr) : ptr_(std::move(ptr)) {
  // nop
}

expected<gateway> gateway::make(configuration cfg,
                                domain_options internal_adaptation,
                                domain_options external_adaptation) {
  return gateway{std::make_unique<impl>(std::move(cfg), &internal_adaptation,
                                        &external_adaptation)};
}

expected<gateway> gateway::make() {
  return gateway{std::make_unique<impl>(configuration{})};
}

// -- setup --------------------------------------------------------------------

void gateway::setup(const caf::actor& internal, const caf::actor& external) {
  caf::anon_send(internal, atom::join_v, external, filter_type{""});
  caf::anon_send(external, atom::join_v, internal, filter_type{""});
}

void gateway::shutdown() {
  anon_send(internal_core(), atom::shutdown_v);
  anon_send(external_core(), atom::shutdown_v);
}

// -- properties ---------------------------------------------------------------

const caf::actor& gateway::internal_core() const noexcept {
  return ptr_->internal;
}

const caf::actor& gateway::external_core() const noexcept {
  return ptr_->external;
}

const configuration& gateway::config() const noexcept {
  return ptr_->cfg;
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
  using pub_fun = caf::expected<uint16_t> (*)(const caf::actor& hdl, uint16_t,
                                              const char*, bool);
  pub_fun publish;
  if (ptr_->cfg.options().disable_ssl)
    publish = caf::io::publish<caf::actor>;
  else
    publish = caf::openssl::publish<caf::actor>;
  if (auto res = publish(core, port, addr, true))
    return *res;
  return 0;
}

std::map<caf::uri, error>
gateway::peer(const std::vector<caf::uri>& internal_peers,
              const std::vector<caf::uri>& external_peers,
              timeout::seconds retry) {
  std::map<caf::uri, error> failures;
  caf::scoped_actor self{ptr_->sys};
  auto f = [&](const auto& peers, const auto& core) {
    for (const auto& peer : peers) {
      if (auto info = to<network_info>(peer)) {
        info->retry = retry;
        self->request(core, caf::infinite, atom::peer_v, std::move(*info))
          .receive(
            [&](const caf::actor&) {
              // success
            },
            [&](caf::error& err) { failures.emplace(peer, err); });
      } else {
        failures.emplace(peer, ec::conversion_failed);
      }
    }
  };
  f(internal_peers, ptr_->internal);
  f(external_peers, ptr_->external);
  return failures;
}

} // namespace broker
