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
#include <caf/net/tcp_accept_socket.hpp>
#include <caf/net/web_socket/server.hpp>
#include <caf/settings.hpp>
#include <caf/uri.hpp>

// TODO: this is very low-level code that becomes obsolete once we switch to
//       CAF 0.19. When upgrading: drop this code and switch to the new API.

namespace broker::internal::web_socket {

struct trait_t {
  using value_type = caf::net::web_socket::frame;

  caf::error init(const caf::settings&) {
    return caf::none;
  }

  bool converts_to_binary(const caf::net::web_socket::frame&) {
    return false; // We use text messages exclusively.
  }

  bool convert(const caf::net::web_socket::frame&, caf::byte_buffer&) {
    return false; // Never serialize to binary.
  }

  bool convert(caf::const_byte_span, caf::net::web_socket::frame&) {
    return false; // Reject binary messages.
  }

  bool convert(const caf::net::web_socket::frame& frame,
               std::vector<char>& buf) {
    if (frame.is_text()) {
      auto text = frame.as_text();
      buf.insert(buf.end(), text.begin(), text.end());
      return true;
    }
    return false;
  }

  bool convert(caf::string_view input, caf::net::web_socket::frame& frame) {
    frame =
      caf::net::web_socket::frame(std::string_view{input.data(), input.size()});
    return true;
  }
};

template <class OnRequest>
class acceptor_factory {
public:
  explicit acceptor_factory(caf::net::openssl::ctx_ptr ctx,
                            OnRequest on_request)
    : ctx_(std::move(ctx)), on_request_(std::move(on_request)) {
    // nop
  }

  caf::error init(caf::net::socket_manager*, const caf::settings&) {
    return caf::none;
  }

  template <class Socket>
  caf::net::socket_manager_ptr make(Socket fd, caf::net::multiplexer* mpx) {
    using trait_t = caf::detail::ws_accept_trait<OnRequest>;
    using value_type = typename trait_t::value_type;
    using app_t =
      caf::net::message_flow_bridge<value_type, trait_t,
                                    caf::tag::mixed_message_oriented>;
    using caf::net::openssl_transport;
    using stack_t = openssl_transport<caf::net::web_socket::server<app_t>>;
    auto policy = caf::net::openssl::policy::make(ctx_, fd);
    auto on_success = [cb = on_request_](caf::net::stream_socket sfd,
                                         caf::net::multiplexer* ptr,
                                         caf::net::openssl::policy policy) {
      return caf::net::make_socket_manager<stack_t>(sfd, ptr, std::move(policy),
                                                    trait_t{cb});
    };
    auto on_error = [](const caf::error& reason) {
      log::network::info("wss-handshake-failed",
                         "SSL handshake on WebSocket failed: {}", reason);
    };
    return caf::net::openssl::async_accept(fd, mpx, std::move(policy),
                                           on_success, on_error);
  }

  void abort(const caf::error& err) {
    // nop
  }

private:
  caf::net::openssl::ctx_ptr ctx_;
  OnRequest on_request_;
};

template <class Socket, class OnRequest>
void ssl_accept(caf::net::multiplexer& mpx, Socket fd,
                caf::net::openssl::ctx_ptr ctx, OnRequest on_request,
                size_t limit = 0) {
  using caf::net::make_socket_manager;
  using factory_t = acceptor_factory<OnRequest>;
  using impl_t = caf::net::connection_acceptor<Socket, factory_t>;
  auto factory = factory_t{std::move(ctx), std::move(on_request)};
  auto ptr = make_socket_manager<impl_t>(fd, &mpx, limit, std::move(factory));
  mpx.init(ptr);
}

expected<uint16_t> launch(caf::actor_system& sys,
                          const openssl_options_ptr& ssl_cfg, std::string addr,
                          uint16_t port, bool reuse_addr,
                          const std::string& allowed_path,
                          on_connect_t on_connect) {
  log::network::debug("ws-start",
                      "launching WebSocket server on port {} with path {}",
                      port, allowed_path);
  using namespace std::literals;
  // Open up the port.
  caf::uri::authority_type auth;
  auth.host = std::move(addr);
  auth.port = port;
  auto fd = caf::net::make_tcp_accept_socket(auth, reuse_addr);
  if (!fd) {
    log::network::error("ws-start-failed",
                        "failed to open WebSocket on port {} -> {}", port,
                        fd.error());
    return {facade(fd.error())};
  }
  auto actual_port = caf::net::local_port(*fd);
  if (!actual_port) {
    log::network::error("ws-start-failed",
                        "failed to retrieve actual port from socket: {}",
                        actual_port.error());
    return {facade(actual_port.error())};
  }
  // Callback for connecting the flows.
  using frame_t = caf::net::web_socket::frame;
  using consumer_res_t = caf::async::consumer_resource<frame_t>;
  using producer_res_t = caf::async::producer_resource<frame_t>;
  using res_t =
    caf::expected<std::tuple<consumer_res_t, producer_res_t, trait_t>>;
  auto on_request = [cb = std::move(on_connect),
                     allowed_path](const caf::settings& hdr) {
    auto path = caf::get_or(hdr, "web-socket.path", "");
    if (path == allowed_path) {
      using caf::async::make_spsc_buffer_resource;
      auto [pull1, push1] = make_spsc_buffer_resource<frame_t>();
      auto [pull2, push2] = make_spsc_buffer_resource<frame_t>();
      connect_event_t ev{std::move(pull2), std::move(push1)};
      cb(hdr, ev);
      return res_t{std::make_tuple(pull1, push2, trait_t{})};
    } else {
      log::network::debug("ws-rejected",
                          "rejected JSON client on invalid path {}", path);
      return res_t{caf::make_error(caf::sec::invalid_argument,
                                   "invalid path; try " + allowed_path)};
    }
  };
  // Launch the WebSocket and dispatch to on_connect.
  namespace ws = caf::net::web_socket;
  if (auto ctx = ssl_context_from_cfg(ssl_cfg)) {
    log::network::info("wss-run",
                       "launching WebSocket server with SSL on port {}",
                       *actual_port);
    ssl_accept(sys.network_manager().mpx(), *fd, std::move(ctx),
               std::move(on_request));
  } else {
    log::network::info("ws-run",
                       "launching WebSocket server (no SSL) on port {}",
                       *actual_port);
    ws::accept(sys.network_manager().mpx(), *fd, on_request);
  }
  return *actual_port;
}

} // namespace broker::internal::web_socket
