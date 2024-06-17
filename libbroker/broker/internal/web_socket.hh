#pragma once

#include "broker/configuration.hh"
#include "broker/fwd.hh"

#include <caf/async/fwd.hpp>
#include <caf/fwd.hpp>
#include <caf/net/accept_event.hpp>
#include <caf/net/fwd.hpp>

#include <functional>

namespace broker::internal::web_socket {

struct client_info {
  std::string user_agent;
  uint16_t remote_port;
  std::string remote_address;
};

using accept_event =
  caf::net::accept_event<caf::net::web_socket::frame, client_info>;

using pull_t = caf::async::consumer_resource<accept_event>;

using on_connect_t = std::function<void(pull_t)>;

expected<uint16_t> launch(caf::actor_system& sys,
                          const openssl_options& ssl_cfg, std::string addr,
                          uint16_t port, bool reuse_addr,
                          const std::string& allowed_path,
                          on_connect_t on_connect);

} // namespace broker::internal::web_socket
