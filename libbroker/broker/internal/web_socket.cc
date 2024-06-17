#include "broker/internal/web_socket.hh"

#include "broker/expected.hh"
#include "broker/internal/connector.hh"
#include "broker/internal/native.hh"
#include "broker/logger.hh"

#include <caf/async/spsc_buffer.hpp>
#include <caf/config_value.hpp>
#include <caf/cow_string.hpp>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/net/middleman.hpp>
#include <caf/net/ssl/context.hpp>
#include <caf/net/tcp_accept_socket.hpp>
#include <caf/net/web_socket/acceptor.hpp>
#include <caf/net/web_socket/with.hpp>
#include <caf/settings.hpp>
#include <caf/uri.hpp>

namespace broker::internal::web_socket {

expected<uint16_t> launch(caf::actor_system& sys,
                          const openssl_options& ssl_cfg, std::string addr,
                          uint16_t port, bool reuse_addr,
                          const std::string& allowed_path,
                          on_connect_t on_connect) {
  using namespace std::literals;
  log::network::debug("ws-start",
                      "launching WebSocket server on port {} with path {}",
                      port, allowed_path);
  namespace ws = caf::net::web_socket;
  namespace ssl = caf::net::ssl;
  auto cstr_or_null = [](const std::string& str) -> const char* {
    return str.empty() ? nullptr : str.c_str();
  };
  auto res =
    ws::with(sys)
      // Optionally enable TLS.
      .context(ssl::context::enable(ssl_cfg.authentication_enabled())
                 .and_then(ssl::emplace_server(ssl::tls::v1_2))
                 .and_then(ssl::use_certificate_file_if(
                   cstr_or_null(ssl_cfg.certificate), ssl::format::pem)))
      // Pass parameters to the acceptor.
      .accept(port, addr, reuse_addr)
      // Check the path for incoming connections.
      .on_request([allowed_path](ws::acceptor<client_info>& acc) {
        auto& hdr = acc.header();
        if (hdr.path() == allowed_path) {
          client_info info;
          if (hdr.has_field("user-agent")) {
            info.user_agent = hdr.field("user-agent");
          } else {
            info.user_agent = "null";
          }
          acc.accept(std::move(info));
          return;
        }
        auto err = caf::make_error(caf::sec::invalid_argument,
                                   "invalid path; try " + allowed_path);
        acc.reject(err);
      })
      // Dispatch to on_connect.
      .start(on_connect);
  if (!res)
    return facade(res.error());
  return port;
}

} // namespace broker::internal::web_socket
