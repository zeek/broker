// Simple program that connects to some peers on startup. When receiving all
// three peering events, we call endpoint::peers(), print the result and quit.

#include "broker/configuration.hh"
#include "broker/convert.hh"
#include "broker/endpoint.hh"
#include "broker/internal/configuration_access.hh"
#include "broker/internal/endpoint_access.hh"

#include <caf/actor_system_config.hpp>
#include <caf/config_option_adder.hpp>
#include <caf/ip_address.hpp>

#include <iostream>
#include <string>
#include <type_traits>
#include <vector>

using namespace broker;
using namespace std::literals;

using uri_list = std::vector<caf::uri>;

// -- URI support for Broker ---------------------------------------------------

namespace broker {

bool convert(const caf::uri& from, network_info& to) {
  if (from.empty())
    return false;
  if (from.scheme() != "tcp")
    return false;
  const auto& auth = from.authority();
  if (auth.empty())
    return false;
  auto f = [](const auto& x) {
    if constexpr (std::is_same_v<std::string, std::decay_t<decltype(x)>>) {
      return x;
    } else {
      return to_string(x);
    }
  };
  to.address = visit(f, auth.host);
  to.port = auth.port;
  return true;
}

} // namespace broker

// -- convenience get_or and get_if overloads ----------------------------------

template <class T>
auto get_or(broker::endpoint& ep, std::string_view key,
            const T& default_value) {
  auto& native_cfg = broker::internal::endpoint_access(&ep).cfg();
  return caf::get_or(native_cfg, key, default_value);
}

// -- program options ----------------------------------------------------------

void extend_config(broker::configuration& broker_cfg) {
  auto& cfg = broker::internal::configuration_access(&broker_cfg).cfg();
  caf::config_option_adder{cfg.custom_options(), "global"} //
    .add<uri_list>("peers", "list of peers we connect to on startup in "
                            "<tcp://$host:$port> notation");
}

// -- main ---------------------------------------------------------------------

int main(int argc, char** argv) {
  broker::endpoint::system_guard sys_guard; // Initialize global state.
  setvbuf(stdout, NULL, _IOLBF, 0);         // Always line-buffer stdout.
  // Parse CLI parameters using our config.
  broker::configuration cfg{broker::skip_init};
  extend_config(cfg);
  try {
    cfg.init(argc, argv);
  } catch (std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return EXIT_FAILURE;
  }
  if (cfg.cli_helptext_printed())
    return EXIT_SUCCESS;
  // Connect to peers.
  broker::endpoint ep{std::move(cfg)};
  auto peers = get_or(ep, "peers", uri_list{});
  if (peers.empty()) {
    std::cerr << "*** mandatory argument missing: peers\n";
    return EXIT_FAILURE;
  }
  for (auto& peer : peers) {
    auto addr = broker::to<broker::network_info>(peer);
    if (!addr) {
      std::cerr << "*** unrecognized URI: " << peer.str() << "\n";
      return EXIT_FAILURE;
    }
    if (!ep.peer(*addr)) {
      std::cerr << "*** unable to connect to: " << peer.str() << '\n';
      return EXIT_FAILURE;
    }
  }
  // Print each peering status. We don't print the ID on purpose, because it's
  // a runtime-generated ID.
  for (auto& info : ep.peers()) {
    std::cout << broker::to_string(info.status) << '\n';
  }
  return EXIT_SUCCESS;
}
