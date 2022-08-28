#pragma once

#include "broker/configuration.hh"
#include "broker/fwd.hh"

#include <caf/async/fwd.hpp>
#include <caf/fwd.hpp>

#include <functional>

namespace broker::internal::web_socket {

using pull_t = caf::async::consumer_resource<caf::cow_string>;
using push_t = caf::async::producer_resource<caf::cow_string>;

using connect_event_t = std::pair<pull_t, push_t>;

using on_connect_t =
  std::function<void(const caf::settings&, connect_event_t&)>;

expected<uint16_t> launch(caf::actor_system& sys,
                          const openssl_options_ptr& ssl_cfg, std::string addr,
                          uint16_t port, bool reuse_addr,
                          const std::string& allowed_path,
                          on_connect_t on_connect);

} // namespace broker::internal::web_socket
