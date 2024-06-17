#include "broker/internal/connector.hh"

#include "broker/detail/assert.hh"
#include "broker/detail/overload.hh"
#include "broker/endpoint.hh"
#include "broker/error.hh"
#include "broker/filter_type.hh"
#include "broker/format/bin.hh"
#include "broker/internal/type_id.hh"
#include "broker/internal/wire_format.hh"
#include "broker/lamport_timestamp.hh"
#include "broker/logger.hh"
#include "broker/message.hh"

#include <caf/async/spsc_buffer.hpp>
#include <caf/binary_deserializer.hpp>
#include <caf/config.hpp>
#include <caf/detail/scope_guard.hpp>
#include <caf/expected.hpp>
#include <caf/net/fwd.hpp>
#include <caf/net/lp/framing.hpp>
#include <caf/net/middleman.hpp>
#include <caf/net/pipe_socket.hpp>
#include <caf/net/ssl/context.hpp>
#include <caf/net/ssl/startup.hpp>
#include <caf/net/tcp_accept_socket.hpp>
#include <caf/net/tcp_stream_socket.hpp>

#include <openssl/err.h>
#include <openssl/ssl.h>

#include <cstdio>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

// -- platform setup -----------------------------------------------------------

// clang-format off
#ifdef CAF_WINDOWS
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif // CAF_WINDOWS
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif // NOMINMAX
#  ifdef CAF_MINGW
#    undef _WIN32_WINNT
#    undef WINVER
#    define _WIN32_WINNT WindowsVista
#    define WINVER WindowsVista
#    include <w32api.h>
#  endif // CAF_MINGW
#  include <windows.h>
#  include <winsock2.h>
#else
#  include <sys/types.h>
#  include <sys/socket.h>
#  include <poll.h>
#endif // CAF_WINDOWS
// clang-format on

namespace {

#ifndef POLLRDHUP
#  define POLLRDHUP POLLHUP
#endif

#ifndef POLLPRI
#  define POLLPRI POLLIN
#endif

#ifdef CAF_WINDOWS
// From the MSDN: If the POLLPRI flag is set on a socket for the Microsoft
//                Winsock provider, the WSAPoll function will fail.
constexpr short read_mask = POLLIN;
#else
constexpr short read_mask = POLLIN | POLLPRI;
#endif

constexpr short write_mask = POLLOUT;

constexpr short error_mask = POLLRDHUP | POLLERR | POLLHUP | POLLNVAL;

constexpr short rw_mask = read_mask | write_mask;

std::mutex init_ssl_api_called_mtx;

bool init_ssl_api_called;

class ssl_error : public std::runtime_error {
public:
  using super = std::runtime_error;

  explicit ssl_error(const char* msg) noexcept : super(msg) {
    // nop
    //
  }
};

namespace bin_v1 = broker::format::bin::v1;

} // namespace

namespace broker {

void endpoint::init_ssl_api() {
  std::unique_lock guard{init_ssl_api_called_mtx};
  if (init_ssl_api_called)
    return;
  caf::net::ssl::startup();
}

void endpoint::deinit_ssl_api() {
  std::unique_lock guard{init_ssl_api_called_mtx};
  if (!init_ssl_api_called)
    return;
  caf::net::ssl::cleanup();
}

} // namespace broker

namespace broker::internal {

/// Creates an SSL context for the connector.
std::optional<caf::net::ssl::context>
ssl_context_from_cfg(const openssl_options_ptr& cfg) {
  namespace ssl = caf::net::ssl;
  if (cfg == nullptr) {
    log::network::debug("no-ssl-config", "run without SSL (no SSL config)");
    return {};
  }
  auto result = ssl::context::make(ssl::tls::v1_2);
  if (!result)
    throw ssl_error("failed to create SSL context");
  auto* hdl = static_cast<SSL_CTX*>(result->native_handle());
  if (cfg->authentication_enabled()) {
    log::network::debug("ssl-enable-authentication",
                        "enable SSL authentication");
    // Require valid certificates on both sides.
    if (!cfg->certificate.empty()
        && result->use_certificate_chain_file(cfg->certificate))
      throw ssl_error("failed to load certificate");
    if (!cfg->passphrase.empty()) {
      result->password(cfg->passphrase);
    }
    if (!cfg->key.empty()
        && !result->use_private_key_file(cfg->key, ssl::format::pem))
      throw ssl_error("failed to load private key");
    if (!cfg->cafile.empty() && result->load_verify_file(cfg->cafile))
      throw ssl_error("failed to load trusted CA certificates");
    if (!cfg->capath.empty() && result->add_verify_path(cfg->capath))
      throw ssl_error("failed to load trusted CA certificates");
    result->verify_mode(ssl::verify::peer | ssl::verify::fail_if_no_peer_cert);
    if (SSL_CTX_set_cipher_list(hdl, "HIGH:!aNULL:!MD5") != 1)
      throw ssl_error("failed to set cipher list");
  } else { // No authentication.
    log::network::debug("ssl-disable-authentication",
                        "disable SSL authentication");
    result->verify_mode(ssl::verify::none);
    ERR_clear_error();
#if defined(SSL_CTX_set_ecdh_auto) && (OPENSSL_VERSION_NUMBER < 0x10100000L)
    SSL_CTX_set_ecdh_auto(hdl, 1);
#elif OPENSSL_VERSION_NUMBER < 0x10101000L
    auto ecdh = EC_KEY_new_by_curve_name(NID_secp384r1);
    if (!ecdh)
      throw ssl_error("failed to get ECDH curve");
    SSL_CTX_set_tmp_ecdh(hdl, ecdh);
    EC_KEY_free(ecdh);
#else // OPENSSL_VERSION_NUMBER < 0x10101000L
    SSL_CTX_set1_groups_list(hdl, "P-384");
#endif
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
    const char* cipher = "AECDH-AES256-SHA@SECLEVEL=0";
#else
    const char* cipher = "AECDH-AES256-SHA";
#endif
    if (SSL_CTX_set_cipher_list(hdl, cipher) != 1)
      throw ssl_error("failed to set anonymous cipher");
  }
  // Prohibit outdated and insecure protocols.
  // TODO: SSL_CTX_set_min_proto_version(ctx.get(), TLS1_2_VERSION);
  return std::move(*result);
}

namespace {

// -- implementations for pending connections ----------------------------------

class plain_pending_connection : public pending_connection {
public:
  explicit plain_pending_connection(caf::net::stream_socket fd) : fd_(fd) {
    // nop
  }

  ~plain_pending_connection() override {
    if (fd_ != caf::net::invalid_socket) {
      log::network::debug("close-socket", "close fd {}", fd_.id);
      caf::net::close(fd_);
    }
  }

  caf::error run(caf::actor_system& sys,
                 caf::async::consumer_resource<caf::chunk> pull,
                 caf::async::producer_resource<caf::chunk> push) override {
    log::network::debug("run-plain-connection", "run plain connection on fd {}",
                        fd_.id);
    if (fd_ == caf::net::invalid_socket) {
      return caf::make_error(caf::sec::socket_invalid);
    }
    caf::net::lp::framing::run(sys.network_manager().mpx(), fd_,
                               std::move(pull), std::move(push));
    fd_.id = caf::net::invalid_socket_id;
    return {};
  }

private:
  caf::net::stream_socket fd_;
};

class encrypted_pending_connection : public pending_connection {
public:
  explicit encrypted_pending_connection(caf::net::ssl::connection conn)
    : conn_(std::move(conn)) {
    // nop
  }

  ~encrypted_pending_connection() {
    if (conn_) {
      auto fd = conn_.fd();
      conn_.close();
      log::network::debug("close-socket", "close fd {}", fd.id);
      caf::net::close(fd);
    }
  }

  caf::error run(caf::actor_system& sys,
                 caf::async::consumer_resource<caf::chunk> pull,
                 caf::async::producer_resource<caf::chunk> push) override {
    log::network::debug("run-ssl-connection", "run SSL connection on fd {}",
                        conn_.fd().id);
    if (!conn_.valid()) {
      return caf::make_error(caf::sec::socket_invalid);
    }
    caf::net::lp::framing::run(sys.network_manager().mpx(), std::move(conn_),
                               std::move(pull), std::move(push));
    return {};
  }

private:
  caf::net::ssl::connection conn_;
};

// -- networking and connector setup -------------------------------------------

enum class connector_msg : uint8_t {
  indeterminate, // Needs more data.
  shutdown,      // System is shutting down.
  connect,       // Try connect to a new Broker endpoint.
  listen,        // Allow other Broker endpoint to connect.
  drop,          // Drop an address from the list of known endpoints.
};

template <class... Ts>
caf::byte_buffer to_buf(connector_msg tag, Ts&&... xs) {
  caf::byte_buffer buf;
  buf.reserve(128); // Pre-allocate some space.
  bin_v1::encoder snk{std::back_inserter(buf)};
  auto ok = snk.apply(static_cast<uint8_t>(tag))
            && snk.apply(uint32_t{0}) // Placeholder for the serialized size.
            && (snk.apply(xs) && ...);
  if (!ok) {
    log::network::error("serialization-failed",
                        "failed to serialize arguments");
    throw std::runtime_error("failed to serialize arguments");
  }
  if constexpr (sizeof...(Ts) > 0) {
    auto payload_len = static_cast<uint32_t>(buf.size() - 5);
    bin_v1::encode(payload_len, buf.begin() + 1);
  }
  return buf;
}

template <class... Ts>
std::tuple<Ts...> from_source(caf::binary_deserializer& src) {
  std::tuple<Ts...> result;
  if (!src.apply(result) || src.remaining() != 0)
    throw std::runtime_error{"error while parsing pipe input"};
  return result;
}

class broken_pipe : public std::exception {
public:
  broken_pipe(int flags) noexcept : flags_(flags) {
    // nop
  }

  const char* what() const noexcept override {
    if (flags_ & (POLLRDHUP | POLLHUP))
      return "POLLRDHUP: cannot read from closed pipe";
    else if (flags_ & POLLERR)
      return "POLLERR: cannot write to closed pipe";
    else if (flags_ & POLLNVAL)
      return "POLLNVAL: invalid pipe handle";
    else
      return "failed to read from pipe for an unknown reason";
  }

private:
  int flags_;
};

class pipe_reader {
public:
  pipe_reader(caf::net::pipe_socket sock, bool* done) noexcept
    : sock_(sock), done_(done) {
    // nop
  }

  ~pipe_reader() {
    // nop
  }

  template <class Manager>
  void read(Manager& mgr) {
    for (;;) {
      auto rdres = caf::net::read(sock_, caf::make_span(rd_buf_));
      if (rdres > 0) {
        buf_.insert(buf_.end(), rd_buf_, rd_buf_ + rdres);
        invoke_from_buf(mgr);
      } else if (rdres == 0 || !caf::net::last_socket_error_is_temporary()) {
        throw broken_pipe{POLLHUP};
      }
      // EAGAIN or some other temporary error.
      return;
    }
  }

private:
  template <class Manager>
  void invoke_from_buf(Manager& mgr) {
    while (!buf_.empty()) {
      caf::binary_deserializer src{nullptr, buf_};
      uint8_t tag = 0;
      if (!src.apply(tag))
        throw std::runtime_error{"error while parsing pipe input"};
      auto msg_type = static_cast<connector_msg>(tag);
      if (msg_type == connector_msg::shutdown) {
        log::network::debug("stop-connector",
                            "received shutdown event, stop the connector");
        *done_ = true;
        return;
      }
      if (buf_.size() < 5)
        return; // Try again later.
      uint32_t len = 0;
      if (!src.apply(len) || len == 0)
        throw std::runtime_error{"error while parsing pipe input"};
      if (src.remaining() < len) {
        log::network::debug(
          "wait-for-payload",
          "wait for payload of size {} with {} already received", len,
          src.remaining());
        return; // Try again later.
      }
      src.reset({buf_.data() + 5, len});
      switch (msg_type) {
        case connector_msg::connect: {
          log::network::debug("received-connect-event",
                              "received connect event");
          auto&& [eid, addr] =
            from_source<connector_event_id, network_info>(src);
          mgr.connect(eid, addr);
          break;
        }
        case connector_msg::drop: {
          log::network::debug("received-drop-event", "received drop event");
          auto&& [eid, addr] =
            from_source<connector_event_id, network_info>(src);
          mgr.drop(eid, addr);
          break;
        }
        case connector_msg::listen: {
          log::network::debug("received-listen-event", "received listen event");
          auto&& [eid, host, port, reuse_addr] =
            from_source<connector_event_id, std::string, uint16_t, bool>(src);
          mgr.listen(eid, host, port, reuse_addr);
          break;
        }
        default:
          log::network::error("received-invalid-event",
                              "received invalid event");
          throw std::runtime_error{"error while parsing pipe input"};
      }
      buf_.erase(buf_.begin(), buf_.begin() + 5 + static_cast<ptrdiff_t>(len));
    }
  }

  caf::net::pipe_socket sock_;
  caf::byte_buffer buf_;
  std::byte rd_buf_[512];
  bool* done_;
};

using caf::net::stream_socket;

// Size of the 'constant' part of a handshake message without the leading
// 4-Bytes to encode the payload size.
static constexpr size_t handshake_prefix_size = 17;

// The full size of HELLO and PING messages:
// - 4 Bytes message length.
// - 17 Bytes for the handshake prefix.
// - 1 Byte for the protocol version.
// - 4 Bytes for the magic number.
static constexpr size_t handshake_first_msg_size = handshake_prefix_size + 9;

struct connect_manager;

enum class write_result {
  again,
  want_read,
  stop,
};

enum class read_result {
  again,
  want_write,
  stop,
};

class connect_state : public std::enable_shared_from_this<connect_state> {
public:
  // -- member types -----------------------------------------------------------

  /// A member function pointer for storing the currently active handler.
  using fn_t = bool (connect_state::*)(wire_format::var_msg&);

  /// The policy object for dispatching to native read and write functions.
  using socket_policy =
    std::variant<caf::net::stream_socket, caf::net::ssl::connection>;

  /// Denotes the state of our socket.
  enum class socket_state {
    /// Indicates that the socket is ready for read and write operations.
    running,
    /// Indicates that the socket is accepting an incoming connection.
    accepting,
    /// Indicates that the socket is establishing a connection.
    connecting,
  };

  // -- constructors, destructors, and assignment operators --------------------

  connect_state(connect_manager* mgr) : mgr(mgr), fn(&connect_state::err) {
    wr_buf.reserve(128);
    rd_buf.reserve(128);
    reset(socket_state::running);
  }

  connect_state(connect_manager* mgr, connector_event_id eid, network_info addr)
    : mgr(mgr), fn(&connect_state::err) {
    wr_buf.reserve(128);
    rd_buf.reserve(128);
    reset(socket_state::running);
    event_id = eid;
    this->addr = std::move(addr);
  }

  ~connect_state() {
    log::network::debug(
      "destroy-connection-state",
      "destroy connect_state object with event-id {} and addr {}", event_id,
      addr);
    auto f =
      detail::make_overload([](stream_socket& fd) { caf::net::close(fd); },
                            [](caf::net::ssl::connection& conn) {
                              auto fd = conn.fd();
                              conn.close();
                              caf::net::close(fd);
                            });
    std::visit(f, sck_policy);
  }

  // -- member variables -------------------------------------------------------

  /// Points to the single manager instance owning all state objects.
  connect_manager* mgr;

  /// Keeps track of the state of our socket to allow us to call connect() or
  /// accept() until the socket becomes ready for read and write operations.
  socket_state sck_state = socket_state::running;

  /// Configures the state with a transport layer.
  socket_policy sck_policy;

  /// Keeps track of the size of the payload we are about to read.
  uint32_t payload_size = 0;

  /// Buffer for writing bytes to the socket.
  caf::byte_buffer wr_buf;

  /// Buffer for reading bytes from the socket.
  caf::byte_buffer rd_buf;

  /// Current position in the read buffer.
  size_t read_pos = 0;

  /// The ID of the node we have connected to. Known after 'hello' or
  /// 'version_select'.
  endpoint_id remote_id;

  /// The filter announced by the remote node.
  filter_type remote_filter;

  /// The IP network address to the remote node.
  network_info addr;

  /// The event ID that led to creating this state or invalid if the state was
  /// created in response to an incoming peering attempt.
  connector_event_id event_id = invalid_connector_event_id;

  /// Keeps track of how many times we tried to connect to the remote node.
  size_t connection_attempts = 0;

  /// Stores whether we detected a redundant connection. Either locally or by
  /// receiving a drop_con messages from the remote.
  bool redundant = false;

  /// Stores whether we called `psm.insert(remote_id, ...)`. If true, we must
  /// erase that state again when aborting.
  bool added_peer_status = false;

  /// Stores pointers to connect states that tried to start a handshake process
  /// while this state had already started it. We store these pointers to delay
  /// the drop_conn message. Otherwise, the drop_conn message might arrive
  /// before handshake messages and the remote side may get confused.
  std::vector<std::shared_ptr<connect_state>> redundant_connections;

  /// Stores the currently active handler.
  fn_t fn = nullptr;

  // -- properties -------------------------------------------------------------

  /// Returns the peer status map for the local endpoint.
  detail::peer_status_map& peer_statuses();

  /// Returns the filter from the local endpoint.
  filter_type local_filter();

  /// Returns the ID of this peer.
  endpoint_id this_peer();

  /// Checks whether the state reached the terminal success state.
  bool reached_fin_state() const noexcept {
    return fn == &connect_state::fin;
  }

  /// Checks whether the state reached the terminal error state.
  bool reached_err_state() const noexcept {
    return fn == &connect_state::err;
  }

  /// Queries whether the handshake completed and all outbound messages are
  /// delivered to the remote node.
  bool done() const noexcept {
    return reached_fin_state() && wr_buf.empty();
  }

  /// Queries whether the state is currently waiting for handshake messages.
  bool performing_handshake() const noexcept {
    fn_t handshake_states[] = {&connect_state::await_hello_or_version_select,
                               &connect_state::await_version_select,
                               &connect_state::await_hello,
                               &connect_state::await_orig_syn,
                               &connect_state::await_resp_syn_ack,
                               &connect_state::await_orig_ack};
    return std::any_of(std::begin(handshake_states), std::end(handshake_states),
                       [this](auto ptr) { return ptr == fn; });
  }

  auto fd() const {
    return std::visit([](auto& hdl) { return caf::net::get_socket_id(hdl); },
                      sck_policy);
  }

  // -- lifetime management ----------------------------------------------------

  /// Resets the state after a connection attempt has failed before trying to
  /// reconnect.
  void reset(socket_state st) {
    redundant = false;
    if (added_peer_status) {
      auto& psm = peer_statuses();
      log::network::debug("added-peer-status", "added peer status {}::{} => ()",
                          remote_id, psm.get(remote_id));
      psm.remove(remote_id);
      added_peer_status = false;
    }
    wr_buf.clear();
    // The first message is always a HELLO or PING message.
    rd_buf.resize(handshake_first_msg_size);
    read_pos = 0;
    payload_size = 0;
    sck_state = st;
    sck_policy = stream_socket{};
    remote_id = endpoint_id::nil();
  }

  /// Assigns a new connection to this state.
  template <class SocketOrSslConnection>
  void assign(SocketOrSslConnection arg) {
    BROKER_ASSERT(caf::net::valid(arg));
    log::network::debug("assign-connection",
                        "initialized new connect_state object for fd {} with "
                        "event-id {} and addr {}",
                        caf::net::get_socket_id(arg), event_id, addr);
    sck_policy = std::move(arg);
  }

  // -- socket operations ------------------------------------------------------

  /// Lifts the socket to a pending connection with any context required from
  /// this state.
  pending_connection_ptr make_pending_connection(stream_socket) {
    using namespace caf::net;
    auto f = detail::make_overload(
      [](stream_socket& fd) -> pending_connection_ptr {
        auto res = std::make_shared<plain_pending_connection>(fd);
        fd.id = invalid_socket_id;
        return res;
      },
      [this](caf::net::ssl::connection& conn) -> pending_connection_ptr {
        auto res =
          std::make_shared<encrypted_pending_connection>(std::move(conn));
        sck_policy = stream_socket{};
        return res;
      });
    return std::visit(f, sck_policy);
  }

  write_result get_write_result(stream_socket, ptrdiff_t) {
    if (caf::net::last_socket_error_is_temporary())
      return write_result::again;
    return write_result::stop;
  }

  write_result get_write_result(caf::net::ssl::connection& conn,
                                ptrdiff_t res) {
    using ssl_errc = caf::net::ssl::errc;
    switch (conn.last_error(res)) {
      case ssl_errc::syscall_failed:
        if (caf::net::last_socket_error_is_temporary()) {
          log::network::debug("ssl-temporary-write-error",
                              "temporary SSL write error on fd {}",
                              conn.fd().id);
          return write_result::again;
        }
        log::network::debug("ssl-write-error", "SSL write error on fd {}: {}",
                            conn.fd().id, conn.last_error_string(res));
        return write_result::stop;
      case ssl_errc::want_write:
        log::network::debug("ssl-want-write",
                            "SSL returned want-write on fd {}", conn.fd().id);
        return write_result::again;
      case ssl_errc::want_read:
        log::network::debug("ssl-want-read", "SSL returned want-read on fd {}",
                            conn.fd().id);
        return write_result::want_read;
      default: // permanent error
        log::network::debug("ssl-write-error", "SSL write error on fd {}: {}",
                            conn.fd().id, conn.last_error_string(res));
        transition(&connect_state::err);
        return write_result::stop;
    }
  }

  read_result get_read_result(stream_socket, ptrdiff_t) {
    if (caf::net::last_socket_error_is_temporary())
      return read_result::again;
    return read_result::stop;
  }

  read_result get_read_result(caf::net::ssl::connection& conn, ptrdiff_t res) {
    using ssl_errc = caf::net::ssl::errc;
    switch (conn.last_error(res)) {
      case ssl_errc::syscall_failed:
        if (caf::net::last_socket_error_is_temporary()) {
          log::network::debug("ssl-temporary-read-error",
                              "temporary SSL read error on fd {}",
                              conn.fd().id);
          return read_result::again;
        }
        log::network::debug("ssl-read-error", "SSL read error on fd {}: {}",
                            conn.fd().id, conn.last_error_string(res));
        return read_result::stop;
      case ssl_errc::want_read:
        log::network::debug("ssl-want-read", "SSL returned want-read on fd {}",
                            conn.fd().id);
        return read_result::again;
      case ssl_errc::want_write:
        log::network::debug("ssl-want-write",
                            "SSL returned want-write on fd {}", conn.fd().id);
        return read_result::want_write;
      default: // permanent error
        log::network::debug("ssl-read-error", "SSL read error on fd {}: {}",
                            conn.fd().id, conn.last_error_string(res));
        transition(&connect_state::err);
        return read_result::stop;
    }
  }

  bool must_read_more();

  template <bool IsServer>
  write_result do_transport_handshake_wr(stream_socket fd);

  template <bool IsServer>
  read_result do_transport_handshake_rd(stream_socket fd);

  ptrdiff_t do_write(stream_socket, caf::span<const std::byte> buf) {
    return std::visit([buf](auto& arg) { return caf::net::write(arg, buf); },
                      sck_policy);
  }

  ptrdiff_t do_read(stream_socket fd, caf::span<std::byte> buf) {
    return std::visit([buf](auto& arg) { return caf::net::read(arg, buf); },
                      sck_policy);
  }

  write_result continue_writing(stream_socket fd) {
    switch (sck_state) {
      case socket_state::accepting:
        log::network::debug("accepting-socket", "accepting connection on fd {}",
                            fd.id);
        return do_transport_handshake_wr<true>(fd);
      case socket_state::connecting:
        log::network::debug("connecting-socket", "connecting to peer on fd {}",
                            fd.id);
        return do_transport_handshake_wr<false>(fd);
      default:
        break;
    }
    if (wr_buf.empty())
      return write_result::stop;
    if (auto res = do_write(fd, wr_buf); res > 0) {
      wr_buf.erase(wr_buf.begin(), wr_buf.begin() + res);
      if (wr_buf.empty()) {
        log::network::debug("write-finished",
                            "finished sending a message to peer on fd {}",
                            fd.id);
        return write_result::stop;
      } else {
        return write_result::again;
      }
    } else if (res < 0) {
      return std::visit(
        [this, res](auto& arg) {
          // Also transitions to the error state if necessary.
          return get_write_result(arg, res);
        },
        sck_policy);
    } else {
      // Socket closed (treat as error).
      log::network::debug("socket-closed", "fd {} closed by peer", fd.id);
      transition(&connect_state::err);
      return write_result::stop;
    }
  }

  write_result continue_writing(caf::net::socket_id fd) {
    return continue_writing(stream_socket{fd});
  }

  read_result continue_reading(stream_socket fd) {
    switch (sck_state) {
      case socket_state::accepting:
        return do_transport_handshake_rd<true>(fd);
      case socket_state::connecting:
        return do_transport_handshake_rd<false>(fd);
      default:
        break;
    }
    for (;;) {
      auto read_size = payload_size + 4;
      auto res = do_read(fd, caf::make_span(rd_buf.data() + read_pos,
                                            read_size - read_pos));
      log::network::debug("try-read-more-result",
                          "do_read returned {} on fd {} (wanted {} more bytes)",
                          res, fd.id, read_size);
      if (res < 0) {
        return std::visit(
          [this, res](auto& arg) {
            // Also transitions to the error state if necessary.
            return get_read_result(arg, res);
          },
          sck_policy);
      } else if (res == 0) {
        // Socket closed (treat as error).
        log::network::debug("socket-closed", "fd {} closed by peer", fd.id);
        transition(&connect_state::err);
        return read_result::stop;
      } else {
        auto ures = static_cast<size_t>(res);
        read_pos += ures;
        if (read_pos == 4) {
          BROKER_ASSERT(payload_size == 0);
          caf::binary_deserializer src{nullptr, rd_buf};
          [[maybe_unused]] auto ok = src.apply(payload_size);
          BROKER_ASSERT(ok);
          if (payload_size == 0) {
            log::network::debug("empty-payload",
                                "received message with "
                                "payload size 0 on fd {}, drop",
                                fd.id);
            transition(&connect_state::err);
            return read_result::stop;
          }
          log::network::debug("wait-for-payload",
                              "wait for payload of size {} on fd {}",
                              payload_size, fd.id);
          rd_buf.resize(payload_size + 4);
          return continue_reading(fd);
        } else if (read_pos == read_size) {
          // Double check the payload size.
          uint32_t pl_size = 0;
          caf::binary_deserializer src{nullptr, rd_buf};
          [[maybe_unused]] auto ok = src.apply(pl_size);
          BROKER_ASSERT(ok);
          if (pl_size != payload_size) {
            log::network::debug("invalid-payload-size",
                                "expected payload size {} on fd {} but got {}",
                                payload_size, fd.id, pl_size);
            transition(&connect_state::err);
            return read_result::stop;
          }
          // Read next message.
          BROKER_ASSERT(rd_buf.size() == payload_size + 4);
          auto bytes = caf::make_span(rd_buf).subspan(4); // Skip the size.
          auto msg = wire_format::decode(bytes);
          if (std::holds_alternative<wire_format::var_msg_error>(msg)) {
            auto& [ec, descr] = std::get<wire_format::var_msg_error>(msg);
            log::network::debug("reject-connection",
                                "reject connection on fd {}: {} -> {}", fd.id,
                                ec, descr);
            send(wire_format::make_drop_conn_msg(this_peer(), ec,
                                                 std::move(descr)));
            transition(&connect_state::err);
            return read_result::stop;
          }
          if (!(*this.*fn)(msg))
            return read_result::stop;
          payload_size = 0;
          read_pos = 0;
          return reached_fin_state() ? read_result::stop : read_result::again;
        } else {
          return read_result::again;
        }
      }
    }
  }

  read_result continue_reading(caf::net::socket_id fd) {
    return continue_reading(stream_socket{fd});
  }

  // -- FSM --------------------------------------------------------------------

  /// Transitions to a new state handler.
  void transition(fn_t f) {
    fn = f;
    if (f == &connect_state::fin) {
      if (!redundant_connections.empty()) {
        auto msg = wire_format::make_drop_conn_msg(this_peer(),
                                                   ec::redundant_connection,
                                                   "redundant connection");
        for (auto& conn : redundant_connections) {
          conn->send(msg);
          conn->transition(&connect_state::fin);
        }
        redundant_connections.clear();
      }
    } else if (f == &connect_state::err) {
      if (added_peer_status) {
        auto& psm = peer_statuses();
        log::network::debug("added-peer-status",
                            "added peer status {}::{} => ()", remote_id,
                            psm.get(remote_id));
        psm.remove(remote_id);
        added_peer_status = false;
      }
    }
  }

  /// Tries to update the status for a peer to connecting.
  /// @returns `true` if the state has been updated and the caller may proceed
  ///          with the handshake process, `false` otherwise and this function
  ///          transitioned to one of `fin`, `paused` or `err`.
  bool proceed_with_handshake(endpoint_id id, bool is_originator);

  template <class T>
  void send(const T& what);

  bool handle(wire_format::drop_conn_msg& msg);

  bool await_hello_or_version_select(wire_format::var_msg& msg);

  bool await_hello(wire_format::var_msg& msg);

  bool await_version_select(wire_format::var_msg& msg);

  bool await_orig_syn(wire_format::var_msg& msg);

  bool await_resp_syn_ack(wire_format::var_msg& msg);

  bool await_orig_ack(wire_format::var_msg& msg);

  /// Connections enter this state when detecting a redundant connection. From
  /// here, they transition to FIN after sending drop_conn eventually.
  bool paused(wire_format::var_msg&) {
    log::network::error("read-after-pause",
                        "tried processing a message in state PAUSE on fd {}",
                        fd());
    return false;
  }

  /// The terminal state for success.
  bool fin(wire_format::var_msg&) {
    log::network::error("read-after-fin",
                        "tried processing a message in state FIN on fd {}",
                        fd());
    return false;
  }

  /// The terminal state for errors.
  bool err(wire_format::var_msg&) {
    log::network::error("read-after-err",
                        "tried processing a message in state ERR on fd {}",
                        fd());
    return false;
  }
};

using connect_state_ptr = std::shared_ptr<connect_state>;

template <class... Ts>
connect_state_ptr make_connect_state(Ts&&... xs) {
  return std::make_shared<connect_state>(std::forward<Ts>(xs)...);
}

struct connect_manager {
  /// Our pollset.
  std::vector<pollfd> fdset;

  /// Stores state objects that wait for their next retry.
  std::multimap<caf::timestamp, connect_state_ptr> retry_schedule;

  /// Stores state objects that are currently performing handshakes.
  std::unordered_map<caf::net::socket_id, connect_state_ptr> pending;

  /// Tags socket IDs that belong to an acceptor.
  std::unordered_set<caf::net::socket_id> acceptors;

  /// We cannot add elements from the pollset while iterating it, so we push new
  /// entries to this container instead and insert after each loop iteration.
  std::vector<pollfd> pending_fdset;

  /// Wraps the callbacks for handshake completion.
  connector::listener* listener;

  /// Grants access to the peer filter.
  shared_filter_type* filter;

  /// Grants access to the thread-safe bookkeeping of peer statuses.
  detail::peer_status_map* peer_statuses_;

  /// Stores the ID of the local peer.
  endpoint_id this_peer;

  /// Stores a pointer to the OpenSSL context when running with SSL enabled.
  caf::net::ssl::context ssl_ctx;

  connect_manager(endpoint_id this_peer, connector::listener* ls,
                  shared_filter_type* filter,
                  detail::peer_status_map* peer_statuses,
                  std::optional<caf::net::ssl::context>&& ctx)
    : listener(ls),
      filter(filter),
      peer_statuses_(peer_statuses),
      this_peer(this_peer),
      ssl_ctx(nullptr) {
    if (ctx) {
      ssl_ctx = std::move(*ctx);
    }
  }

  connect_manager(const connect_manager&) = delete;

  connect_manager& operator=(const connect_manager&) = delete;

  /// Returns the relative timeout for the next retry in milliseconds or -1.
  int next_timeout_ms() {
    if (retry_schedule.empty())
      return -1;
    auto now = caf::make_timestamp();
    auto timeout = retry_schedule.begin()->first;
    if (timeout > now) {
      namespace sc = std::chrono;
      auto count = sc::duration_cast<sc::milliseconds>(timeout - now).count();
      return static_cast<int>(count);
    }
    return 0;
  }

  pollfd* find_pollfd(caf::net::socket_id fd) {
    for (auto& ref : fdset)
      if (ref.fd == fd)
        return std::addressof(ref);
    for (auto& ref : pending_fdset)
      if (ref.fd == fd)
        return std::addressof(ref);
    return nullptr;
  }

  void register_fd(connect_state* ptr, short event) {
    auto pred = [ptr](const auto& kvp) { return kvp.second.get() == ptr; };
    auto e = pending.end();
    if (auto i = std::find_if(pending.begin(), e, pred); i != e) {
      log::network::debug("register-fd", "register fd {} for {}", i->first,
                          event == read_mask ? "reading" : "writing");
      if (auto fds_ptr = find_pollfd(i->first)) {
        fds_ptr->events = static_cast<short>(fds_ptr->events | event);
      } else {
        pending_fdset.emplace_back(pollfd{i->first, event, 0});
      }
    } else {
      log::network::error(
        "register-fd-failed",
        "called register_writing for an unknown connect state");
    }
  }

  connect_state* find_pending_handshake(endpoint_id peer) {
    for (auto& kvp : pending) {
      auto st = kvp.second.get();
      if (st->performing_handshake() && st->remote_id == peer)
        return st;
    }
    return nullptr;
  }

  void register_writing(connect_state* ptr) {
    register_fd(ptr, write_mask);
  }

  void register_reading(connect_state* ptr) {
    register_fd(ptr, read_mask);
  }

  void connect(connect_state_ptr state) {
    using namespace std::literals;
    BROKER_ASSERT(!state->addr.address.empty());
    caf::uri::authority_type authority;
    authority.host = state->addr.address;
    authority.port = state->addr.port;
    log::network::debug("try-connect",
                        "try connecting to {} with a timeout of 1s", authority);
    auto event_id = state->event_id;
    if (auto sock = caf::net::make_connected_tcp_stream_socket(authority, 1s)) {
      log::network::debug("connect-ok",
                          "established connection on fd {} to {} "
                          "(initiate handshake)",
                          sock->id, authority);
      if (auto err = caf::net::nonblocking(*sock, true)) {
        auto err_str = to_string(err);
        fprintf(stderr,
                "failed to set socket %d to nonblocking (line %d): %s\n",
                (int) sock->id, __LINE__, err_str.c_str());
        ::abort();
      }
      if (auto err = caf::net::allow_sigpipe(*sock, false)) {
        auto err_str = to_string(err);
        fprintf(stderr,
                "failed to disable sigpipe on socket %d (line %d): %s\n",
                (int) sock->id, __LINE__, err_str.c_str());
        ::abort();
      }
      if (auto i = pending.find(sock->id); i != pending.end()) {
        log::network::warning("stale-connection",
                              "fd {} already associated to state object -> "
                              "assume stale state and drop it!",
                              sock->id);
        pending.erase(i);
      }
      short mask = 0;
      if (ssl_ctx) {
        mask = write_mask; // SSL wants to write first.
        auto conn = ssl_ctx.new_connection(*sock);
        if (!conn) {
          log::network::warning("ssl-connect-failed",
                                "failed to start an SSL connection on fd {}",
                                sock->id);
          return;
        }
        state->reset(connect_state::socket_state::connecting);
        state->assign(std::move(*conn));
      } else {
        mask = read_mask;
        state->reset(connect_state::socket_state::running);
        state->assign(*sock);
      }
      pending.emplace(sock->id, state);
      pending_fdset.push_back({sock->id, mask, 0});
      state->transition(&connect_state::await_hello_or_version_select);
      state->send(wire_format::make_hello_msg(this_peer));
    } else {
      auto retry_interval = state->addr.retry;
      if (retry_interval.count() != 0) {
        log::network::debug("connect-failed-with-retry",
                            "failed to connect to {} -> retry in {}s",
                            authority, retry_interval);
        listener->on_peer_unavailable(state->addr);
        retry_schedule.emplace(caf::make_timestamp() + retry_interval,
                               std::move(state));
      } else if (valid(event_id)) {
        log::network::debug("connect-failed",
                            "failed to connect to {} -> fail (retry disabled)",
                            authority);
        listener->on_error(event_id, caf::make_error(ec::peer_unavailable));
      } else {
        listener->on_peer_unavailable(state->addr);
      }
    }
  }

  /// Registers a new state object for connecting to given address.
  void connect(connector_event_id event_id, const network_info& addr) {
    connect(make_connect_state(this, event_id, addr));
  }

  void listen(connector_event_id event_id, std::string& addr, uint16_t port,
              bool reuse_addr) {
    using namespace std::literals;
    caf::uri::authority_type authority;
    authority.host = addr;
    authority.port = port;
#ifdef BROKER_WINDOWS
    // SO_REUSEADDR behaves quite differently on Windows. CAF currently does not
    // differentiate between UNIX and Windows in this regard, so we'll force
    // this option to false on Windows in the meantime.
    reuse_addr = false;
#endif
    if (auto sock = caf::net::make_tcp_accept_socket(authority, reuse_addr)) {
      if (auto actual_port = caf::net::local_port(*sock)) {
        log::network::debug("started-listening",
                            "started listening on port {} (fd {})",
                            *actual_port, sock->id);
        acceptors.emplace(sock->id);
        pending_fdset.push_back({sock->id, read_mask, 0});
        listener->on_listen(event_id, *actual_port);
      } else {
        log::network::error("local_port-failed",
                            "failed to determine local port: {}",
                            actual_port.error());
        caf::net::close(*sock);
        listener->on_error(event_id, std::move(actual_port.error()));
      }
    } else {
      log::network::error("make_tcp_accept_socket-failed",
                          "make_tcp_accept_socket failed: {}", sock.error());
      listener->on_error(event_id, std::move(sock.error()));
    }
  }

  /// Registers a new state object for connecting to given address.
  void drop(connector_event_id event_id, network_info& addr) {
    // nop
  }

  void finalize(pollfd& entry, connect_state& state) {
    if (!state.redundant) {
      auto conn = state.make_pending_connection(stream_socket{entry.fd});
      listener->on_connection(state.event_id, state.remote_id, state.addr,
                              state.remote_filter, std::move(conn));
    } else {
      listener->on_redundant_connection(state.event_id, state.remote_id,
                                        state.addr);
      close(caf::net::socket{entry.fd});
      entry.events = 0;
    }
  }

  bool must_read_more(pollfd& entry) {
    if (auto i = pending.find(entry.fd); i != pending.end())
      return i->second->must_read_more();
    else
      return false;
  }

  void continue_reading(pollfd& entry) {
    if (auto i = pending.find(entry.fd); i != pending.end()) {
      switch (i->second->continue_reading(entry.fd)) {
        case read_result::again:
          // nop
          break;
        case read_result::stop:
          if (i->second->reached_err_state()) {
            if ((entry.events & write_mask) == 0) {
              abort(entry);
            } else {
              // Leave cleanup to continue_writing.
              log::network::debug("stop-reading", "stop reading on fd {}",
                                  entry.fd);
              entry.events &= ~read_mask;
              return;
            }
          } else {
            log::network::debug("stop-reading", "stop reading on fd {}",
                                entry.fd);
            entry.events &= ~read_mask;
            if (i->second->done()) {
              finalize(entry, *i->second);
              pending.erase(i);
            }
          }
          break;
        case read_result::want_write:
          entry.events = write_mask;
          break;
        default:
          log::network::error("invalid-read-result",
                              "state returned unsupported read_result");
          abort(entry);
      }
    } else if (acceptors.count(entry.fd) != 0) {
      using namespace std::literals;
      auto accept_sock = caf::net::tcp_accept_socket{entry.fd};
      if (auto sock = caf::net::accept(accept_sock)) {
        BROKER_ASSERT(pending.count(sock->id) == 0);
        if (auto err = caf::net::nonblocking(*sock, true)) {
          auto err_str = to_string(err);
          fprintf(stderr,
                  "failed to set socket %d to nonblocking (line %d): %s\n",
                  (int) sock->id, __LINE__, err_str.c_str());
          ::abort();
        }
        if (auto err = caf::net::allow_sigpipe(*sock, false)) {
          auto err_str = to_string(err);
          fprintf(stderr,
                  "failed to disable sigpipe on socket %d (line %d): %s\n",
                  (int) sock->id, __LINE__, err_str.c_str());
          ::abort();
        }
        auto st = make_connect_state(this);
        st->addr.retry = 0s;
        if (auto addr = caf::net::remote_addr(*sock))
          st->addr.address = std::move(*addr);
        if (auto port = caf::net::remote_port(*sock))
          st->addr.port = *port;
        log::network::debug("accepted-new-connection",
                            "accepted new connection from socket {}"
                            " -> register for reading {}",
                            entry.fd, sock->id);
        short mask = 0;
        if (ssl_ctx) {
          mask = write_mask; // SSL wants to write first.
          st->sck_state = connect_state::socket_state::accepting;
          auto conn = ssl_ctx.new_connection(*sock);
          if (!conn) {
            log::network::warning("ssl-accept-failed",
                                  "failed to accept an SSL connection on fd {}",
                                  sock->id);
            return;
          }
          st->assign(std::move(*conn));
        } else {
          mask = read_mask;
          st->sck_state = connect_state::socket_state::running;
          st->assign(std::move(*sock));
        }
        pending_fdset.push_back({sock->id, mask, 0});
        pending.emplace(sock->id, st);
        st->transition(&connect_state::await_hello);
        st->send(wire_format::make_probe_msg());
      }
    } else {
      entry.events &= ~read_mask;
    }
  }

  void continue_writing(pollfd& entry) {
    if (auto i = pending.find(entry.fd); i != pending.end()) {
      switch (i->second->continue_writing(entry.fd)) {
        case write_result::again:
          // nop
          break;
        case write_result::stop:
          if (i->second->reached_err_state()) {
            abort(entry);
            return;
          }
          entry.events &= ~write_mask;
          if (i->second->done()) {
            log::network::debug("peer-state-done",
                                "peer state reports done for fd {}", entry.fd);
            finalize(entry, *i->second);
            pending.erase(i);
          }
          log::network::debug("continue-writing",
                              "continue writing on fd {} done", entry.fd);
          break;
        case write_result::want_read:
          entry.events = read_mask;
          break;
        default:
          log::network::error("invalid-write-result",
                              "state returned unsupported write_result");
          abort(entry);
      }
    } else {
      entry.events &= ~write_mask;
    }
  }

  void abort(pollfd& entry) {
    if (auto i = pending.find(entry.fd); i != pending.end()) {
      auto state = std::move(i->second);
      pending.erase(i);
      if (state->redundant) {
        log::network::debug("drop-redundant-connection",
                            "drop redundant connection on socket {}", entry.fd);
        if (state->event_id != invalid_connector_event_id)
          listener->on_redundant_connection(state->event_id, state->remote_id,
                                            state->addr);
      } else if (state->event_id != invalid_connector_event_id) {
        auto retry_interval = state->addr.retry;
        if (retry_interval.count() > 0) {
          listener->on_peer_unavailable(state->addr);
          retry_schedule.emplace(caf::make_timestamp() + retry_interval,
                                 std::move(state));
          log::network::debug("failed-to-connect",
                              "failed to connect on socket {}, try again in {}",
                              entry.fd, retry_interval);
        } else {
          log::network::debug("failed-to-connect",
                              "failed to connect on socket {} -> give up",
                              entry.fd);
          if (valid(state->event_id)) {
            listener->on_error(state->event_id,
                               caf::make_error(ec::peer_unavailable));
          } else {
            listener->on_peer_unavailable(state->addr);
          }
        }
      } else {
        log::network::debug("incoming-peering-failed",
                            "incoming peering failed on socket {}", entry.fd);
      }
    } else if (auto j = acceptors.find(entry.fd); j != acceptors.end()) {
      log::network::error("acceptor-failed", "acceptor failed on socket {}",
                          entry.fd);
      acceptors.erase(j);
    }
    log::network::debug("close-socket", "close socket {}", entry.fd);
    close(caf::net::socket{entry.fd});
    entry.events = 0;
  }

  int next_timeout() {
    if (retry_schedule.empty()) {
      return -1;
    } else {
      auto now = caf::make_timestamp();
      auto ts = retry_schedule.begin()->first;
      if (ts <= now) {
        return 0;
      } else {
        namespace sc = std::chrono;
        auto ms = sc::duration_cast<sc::milliseconds>(ts - now);
        return static_cast<int>(ms.count());
      }
    }
  }

  void handle_timeouts() {
    auto now = caf::make_timestamp();
    while (!retry_schedule.empty() && retry_schedule.begin()->first <= now) {
      auto i = retry_schedule.begin();
      auto state = std::move(i->second);
      retry_schedule.erase(i);
      connect(std::move(state));
    }
  }

  void prepare_next_cycle() {
    auto is_done = [](auto& x) { return x.events == 0; };
    auto new_end = std::remove_if(fdset.begin(), fdset.end(), is_done);
    if (new_end != fdset.end()) {
#if CAF_LOG_LEVEL >= CAF_LOG_LEVEL_DEBUG
      std::for_each(new_end, fdset.end(), [](auto& x) {
        if (x.fd != detail::invalid_native_socket)
          log::network::debug("drop-completed-socket",
                              "drop completed socket {} from pollset", x.fd);
      });
#endif
      fdset.erase(new_end, fdset.end());
    }
    if (!pending_fdset.empty()) {
      fdset.insert(fdset.end(), pending_fdset.begin(), pending_fdset.end());
      pending_fdset.clear();
    }
  }
};

detail::peer_status_map& connect_state::peer_statuses() {
  return *mgr->peer_statuses_;
}

bool connect_state::must_read_more() {
  if (auto conn = std::get_if<caf::net::ssl::connection>(&sck_policy))
    return conn->buffered() > 0;
  else
    return false;
}

template <bool IsServer>
write_result connect_state::do_transport_handshake_wr(stream_socket fd) {
  if (auto conn = std::get_if<caf::net::ssl::connection>(&sck_policy)) {
    ptrdiff_t res;
    if constexpr (IsServer) {
      res = conn->accept();
      log::network::debug("ssl-accept", "SSL accept returned {} on fd {}", res,
                          fd.id);
    } else {
      res = conn->connect();
      log::network::debug("ssl-connect", "SSL connect returned {} on fd {}",
                          res, fd.id);
    }
    if (res > 0) { // success
      sck_state = socket_state::running;
      mgr->register_reading(this);
      return write_result::again;
    }
    return get_write_result(*conn, res);
  } else {
    log::network::error("invalid-ssl-state-wr",
                        "invalid state: called do_transport_handshake_wr "
                        "on a non-SSL fd {}",
                        fd.id);
    transition(&connect_state::err);
    return write_result::stop;
  }
}

template <bool IsServer>
read_result connect_state::do_transport_handshake_rd(stream_socket fd) {
  if (auto conn = std::get_if<caf::net::ssl::connection>(&sck_policy)) {
    ptrdiff_t res;
    if constexpr (IsServer)
      res = conn->accept();
    else
      res = conn->connect();
    if (res > 0) { // success
      sck_state = socket_state::running;
      if (!wr_buf.empty())
        mgr->register_writing(this);
      return read_result::again;
    } else if (res < 0) { // error
      return get_read_result(fd, res);
    } else { // socket closed
      log::network::debug(
        "socket-closed",
        "socket closed by peer while trying to read the transport handshake");
      transition(&connect_state::err);
      return read_result::stop;
    }
  } else {
    log::network::error("invalid-ssl-state-wr",
                        "invalid state: called do_transport_handshake_rd "
                        "on a non-SSL socket");
    transition(&connect_state::err);
    return read_result::stop;
  }
}

template <class T>
void connect_state::send(const T& what) {
  bin_v1::encoder sink{std::back_inserter(wr_buf)};
  // Store the current writing position for later and add dummy size.
  auto old_size = wr_buf.size();
  uint32_t dummy = 0;
  std::ignore = sink.apply(dummy);
  // Encode the actual message and override the dummy with the actual size.
  std::ignore = wire_format::encode(sink, what);
  auto len = static_cast<uint32_t>(wr_buf.size() - old_size - 4);
  bin_v1::encode(len, wr_buf.begin() + static_cast<ptrdiff_t>(old_size));
  log::network::debug("start-writing",
                      "start writing a {} message on fd {} of size {}", T::tag,
                      fd(), len);
  mgr->register_writing(this);
}

bool connect_state::handle(wire_format::drop_conn_msg& msg) {
  // The remote peer sends this message with error code ec::redundant_connection
  // if we already have a connection established. However, re-connects after an
  // unpeering or other events may produce conflicting views where the remote
  // did not realize yet that this connection no longer exists. Hence, we need
  // to double-check whether we have a connection or peering relation and
  // otherwise we raise an error to trigger retries.
  if (msg.code == static_cast<uint8_t>(ec::redundant_connection)) {
    auto stat = peer_statuses().get(msg.sender_id);
    log::network::debug("drop-redundant-connection",
                        "received drop_conn from {} with peer status {} "
                        "for redundant connection",
                        msg.sender_id, stat);
    switch (stat) {
      case peer_status::connecting:
      case peer_status::connected:
      case peer_status::peered:
        remote_id = msg.sender_id;
        redundant = true;
        transition(&connect_state::fin);
        return true;
      default:
        transition(&connect_state::err);
        return false;
    }
  } else {
    log::network::error("drop-connection-with-error",
                        "received drop_conn from {} with error code {} "
                        "and peer status {}",
                        msg.sender_id, msg.code,
                        peer_statuses().get(msg.sender_id));
    transition(&connect_state::err);
    return false;
  }
}

bool connect_state::await_hello_or_version_select(wire_format::var_msg& msg) {
  auto index = msg.index();
  switch (index) {
    default:
      log::network::error("unexpected-message",
                          "unexpected message of type {} while waiting for "
                          "hello or version selection",
                          index);
      transition(&connect_state::err);
      return false;
    case wire_format::hello_index:
      return await_hello(msg);
    case wire_format::version_select_index:
      return await_version_select(msg);
    case wire_format::drop_conn_index:
      return handle(get<wire_format::drop_conn_msg>(msg));
    case wire_format::probe_index:
      // No further processing required.
      return true;
  }
}

bool connect_state::proceed_with_handshake(endpoint_id id, bool is_originator) {
  auto proceed = [this, id] {
    added_peer_status = true;
    remote_id = id;
    return true;
  };
  auto& psm = peer_statuses();
  auto status = peer_status::connecting;
  if (psm.insert(id, status)) {
    BROKER_ASSERT(status == peer_status::connecting);
    log::network::debug("peer-status-inserted",
                        "inserted peer status {}::{} -> ()", id, status);
    return proceed();
  } else {
    for (;;) { // Repeat until we succeed or fail.
      switch (status) {
        case peer_status::initialized:
          if (psm.update(id, status, peer_status::connecting)) {
            log::network::debug(
              "peer-status-updated",
              "updated peer status {}::initialized -> connecting", id);
            return proceed();
          }
          break;
        case peer_status::connecting:
        case peer_status::reconnecting:
          if (is_originator) {
            if (auto other = mgr->find_pending_handshake(id)) {
              log::network::debug("redundant-connection",
                                  "detected redundant connection, "
                                  "enter paused state");
              BROKER_ASSERT(other != this);
              redundant = true;
              remote_id = id;
              other->redundant_connections.emplace_back(shared_from_this());
              transition(&connect_state::paused);
              return false;
            } else {
              log::network::debug("redundant-connection-failed",
                                  "detected redundant connection but "
                                  "find_pending_handshake failed");
              send(wire_format::make_drop_conn_msg(
                this_peer(), ec::logic_error,
                "detected redundant connection but "
                "find_pending_handshake failed"));
              transition(&connect_state::err);
              return false;
            }
          } else {
            log::network::warning("redundant-responder-connection",
                                  "detected redundant connection to {} "
                                  "at the responder with status {}",
                                  id, status);
            send(wire_format::make_drop_conn_msg(
              this_peer(), ec::logic_error,
              "detected redundant connection at the responder"));
            transition(&connect_state::err);
            return false;
          }
          break;
        case peer_status::connected:
        case peer_status::peered:
          if (is_originator) {
            log::network::debug("redundant-connection",
                                "detected redundant connection "
                                "for connected peer");
            send(wire_format::make_drop_conn_msg(
              this_peer(), ec::redundant_connection, "redundant connection"));
            redundant = true;
            remote_id = id;
            transition(&connect_state::fin);
            return false;
          } else {
            log::network::warning("redundant-responder-connection",
                                  "detected redundant connection to {} "
                                  "at the responder with status {}",
                                  id, status);
            send(wire_format::make_drop_conn_msg(
              this_peer(), ec::logic_error,
              "detected redundant connection at the responder"));
            transition(&connect_state::err);
            return false;
          }
        case peer_status::disconnected:
          if (psm.update(id, status, peer_status::reconnecting)) {
            log::network::debug("disconnected->reconnecting",
                                "peer status {}::disconnected -> reconnecting",
                                id);
            return proceed();
          }
          break;
        case peer_status::unknown:
          // The map returns this state only if the map has been closed.
          send(wire_format::make_drop_conn_msg(this_peer(), ec::shutting_down,
                                               "shutting down"));
          transition(&connect_state::err);
          return false;
        default:
          log::network::error("invalid-peer-status", "invalid peer status: {}",
                              status);
          send(wire_format::make_drop_conn_msg(this_peer(), ec::logic_error,
                                               "invalid peer status"));
          transition(&connect_state::err);
          return false;
      }
    }
  }
}

bool connect_state::await_hello(wire_format::var_msg& msg) {
  auto index = msg.index();
  switch (index) {
    default:
      log::network::error("unexpected-message",
                          "unexpected message of type {} "
                          "while waiting for hello",
                          index);
      transition(&connect_state::err);
      return false;
    case wire_format::drop_conn_index:
      return handle(get<wire_format::drop_conn_msg>(msg));
    case wire_format::probe_index:
      // No further processing required.
      return true;
    case wire_format::hello_index:
      break;
  }
  auto& hello = std::get<wire_format::hello_msg>(msg);
  if (hello.min_version > wire_format::protocol_version) {
    log::network::debug("reject-version",
                        "reject peering: version range not supported");
    send(wire_format::make_drop_conn_msg(this_peer(), ec::peer_incompatible,
                                         "version range not supported"));
    transition(&connect_state::err);
    return false;
  } else if (mgr->this_peer < hello.sender_id) {
    if (proceed_with_handshake(hello.sender_id, true)) {
      send(wire_format::make_version_select_msg(this_peer()));
      send(wire_format::v1::make_originator_syn_msg(local_filter()));
      transition(&connect_state::await_resp_syn_ack);
      return true;
    } else {
      return fn != &connect_state::err;
    }
  } else {
    send(wire_format::make_hello_msg(this_peer()));
    transition(&connect_state::await_version_select);
    return true;
  }
}

bool connect_state::await_version_select(wire_format::var_msg& msg) {
  auto index = msg.index();
  switch (index) {
    default:
      log::network::error("unexpected-message",
                          "unexpected message of type {} "
                          "while waiting for version selection",
                          index);
      transition(&connect_state::err);
      return false;
    case wire_format::drop_conn_index:
      return handle(get<wire_format::drop_conn_msg>(msg));
    case wire_format::probe_index:
      // No further processing required.
      return true;
    case wire_format::version_select_index:
      break;
  }
  auto& vselect = std::get<wire_format::version_select_msg>(msg);
  if (vselect.selected_version != wire_format::protocol_version) {
    send(wire_format::make_drop_conn_msg(this_peer(), ec::peer_incompatible,
                                         "selected version not supported"));
    transition(&connect_state::err);
    return false;
  } else if (proceed_with_handshake(vselect.sender_id, false)) {
    transition(&connect_state::await_orig_syn);
    return true;
  } else {
    return fn != &connect_state::err;
  }
}

bool connect_state::await_orig_syn(wire_format::var_msg& msg) {
  if (!std::holds_alternative<wire_format::v1::originator_syn_msg>(msg)) {
    log::network::error("unexpected-message",
                        "unexpected message of type {} "
                        "while waiting for originator syn",
                        msg.index());
    transition(&connect_state::err);
    return false;
  }
  auto& syn = std::get<wire_format::v1::originator_syn_msg>(msg);
  remote_filter = std::move(syn.filter);
  send(wire_format::v1::make_responder_syn_ack_msg(local_filter()));
  transition(&connect_state::await_orig_ack);
  return true;
}

bool connect_state::await_resp_syn_ack(wire_format::var_msg& msg) {
  if (!std::holds_alternative<wire_format::v1::responder_syn_ack_msg>(msg)) {
    log::network::error("unexpected-message",
                        "unexpected message of type {} "
                        "while waiting for responder syn ack",
                        msg.index());
    transition(&connect_state::err);
    return false;
  }
  auto& syn_ack = std::get<wire_format::v1::responder_syn_ack_msg>(msg);
  remote_filter = std::move(syn_ack.filter);
  auto& psm = peer_statuses();
  auto status = peer_status::connecting;
  if (psm.update(remote_id, status, peer_status::connected)) {
    log::network::debug("connecting->connected",
                        "peer status {}::connecting -> connected", remote_id);
  } else if (status == peer_status::reconnecting
             && psm.update(remote_id, status, peer_status::connected)) {
    log::network::debug("reconnecting->connected",
                        "peer status {}::reconnecting -> connected", remote_id);
  } else {
    log::network::error(
      "broken-syn-ack",
      "received resp_syn_ack, but peer status does not match");
    return false;
  }
  send(wire_format::v1::make_originator_ack_msg());
  transition(&connect_state::fin);
  return true;
}

bool connect_state::await_orig_ack(wire_format::var_msg& msg) {
  if (!std::holds_alternative<wire_format::v1::originator_ack_msg>(msg)) {
    log::network::error("unexpected-message",
                        "unexpected message of type {} "
                        "while waiting for originator ack",
                        msg.index());
    transition(&connect_state::err);
    return false;
  }
  auto& psm = peer_statuses();
  auto status = peer_status::connecting;
  if (psm.update(remote_id, status, peer_status::connected)) {
    log::network::debug("connecting->connected",
                        "peer status {}::connecting -> connected", remote_id);
  } else if (status == peer_status::reconnecting
             && psm.update(remote_id, status, peer_status::connected)) {
    log::network::debug("reconnecting->connected",
                        "peer status {}::reconnecting -> connected", remote_id);
  } else {
    log::network::error("broken-ack",
                        "received orig_ack, but peer status does not match");
    return false;
  }
  transition(&connect_state::fin);
  return true;
}

filter_type connect_state::local_filter() {
  return mgr->filter->read();
}

endpoint_id connect_state::this_peer() {
  return mgr->this_peer;
}

} // namespace

connector::listener::~listener() {
  // nop
}

connector::connector(endpoint_id this_peer, broker_options broker_cfg,
                     openssl_options_ptr ssl_cfg)
  : this_peer_(this_peer),
    broker_cfg_(broker_cfg),
    ssl_cfg_(std::move(ssl_cfg)) {
  // Open the pipe and configure the file descriptors.
  auto fds = caf::net::make_pipe();
  if (!fds) {
    auto err_str = to_string(fds.error());
    fprintf(stderr, "failed to create pipe: %s\n", err_str.c_str());
    abort();
  }
  auto [rd, wr] = *fds;
  if (auto err = caf::net::nonblocking(rd, true)) {
    auto err_str = to_string(err);
    fprintf(stderr,
            "failed to set pipe handle %d to nonblocking (line %d): %s\n",
            __LINE__, (int) rd.id, err_str.c_str());

    ::abort();
  }
  pipe_rd_ = rd.id;
  pipe_wr_ = wr.id;
  // Copy the password into the global buffer if provided.
  // if (ssl_cfg_ && !ssl_cfg_->passphrase.empty()) {
  //   if (ssl_cfg_->passphrase.size() > max_ssl_passphrase_size) {
  //     fprintf(stderr, "SSL passphrase may not exceed %d characters\n",
  //             static_cast<int>(max_ssl_passphrase_size));
  //     ::abort();
  //   }
  //   strncpy(ssl_passphrase_buf, ssl_cfg_->passphrase.c_str(),
  //           max_ssl_passphrase_size);
  // }
}

connector::~connector() {
  namespace cn = caf::net;
  cn::close(cn::pipe_socket{pipe_rd_});
  cn::close(cn::pipe_socket{pipe_wr_});
}

void connector::async_connect(connector_event_id event_id,
                              const network_info& addr) {
  auto buf = to_buf(connector_msg::connect, event_id, addr);
  write_to_pipe(buf);
}

void connector::async_drop(const connector_event_id event_id,
                           const network_info& addr) {
  auto buf = to_buf(connector_msg::drop, event_id, addr);
  write_to_pipe(buf);
}

void connector::async_listen(connector_event_id event_id,
                             const std::string& address, uint16_t port,
                             bool reuse_addr) {
  auto buf = to_buf(connector_msg::listen, event_id, address, port, reuse_addr);
  write_to_pipe(buf);
}

void connector::async_shutdown() {
  auto buf = to_buf(connector_msg::shutdown);
  write_to_pipe(buf, true);
}

void connector::write_to_pipe(caf::span<const std::byte> bytes,
                              bool shutdown_after_write) {
  std::unique_lock guard{mtx_};
  if (shutting_down_) {
    if (!shutdown_after_write) {
      const char* errmsg = "failed to write to the pipe: shutting down";
      log::network::error("write-to-pipe", "{}", errmsg);
      throw std::runtime_error(errmsg);
    } else {
      // Calling async_shutdown multiple times is OK.
      return;
    }
  }
  auto res = caf::net::write(caf::net::pipe_socket{pipe_wr_}, bytes);
  if (res != static_cast<ptrdiff_t>(bytes.size())) {
    const char* errmsg = "wrong number of bytes written to the pipe";
    log::network::error("write-to-pipe", "{}", errmsg);
    throw std::runtime_error(errmsg);
  }
  if (shutdown_after_write)
    shutting_down_ = true;
}

void connector::init(std::unique_ptr<listener> sub, shared_filter_ptr filter,
                     detail::shared_peer_status_map_ptr peer_statuses) {
  BROKER_ASSERT(sub != nullptr);
  BROKER_ASSERT(filter != nullptr);
  std::unique_lock guard{mtx_};
  if (sub_ != nullptr)
    throw std::logic_error("connector::init called twice");
  BROKER_ASSERT(filter_ == nullptr);
  sub_ = std::move(sub);
  filter_ = std::move(filter);
  peer_statuses_ = std::move(peer_statuses);
  sub_cv_.notify_all();
}

void connector::run() {
  // Wait for subscriber and filter before starting the loop.
  listener* sub = nullptr;
  shared_filter_type* filter = nullptr;
  {
    std::unique_lock guard{mtx_};
    while (sub_ == nullptr)
      sub_cv_.wait(guard);
    sub = sub_.get();
    filter = filter_.get();
  }
  try {
    run_impl(sub, filter);
  } catch (const std::exception& ex) {
    log::network::error("exception", "exception in connector::run: {}",
                        ex.what());
  }
  sub->on_shutdown();
}

namespace {

enum class ssl_lib_state_t { uninitialized, initialized, deinitialized };

std::mutex ssl_init_mtx_;
ssl_lib_state_t ssl_lib_state;

/// Initializes and uninitializes the library when needed.
struct ssl_lib_guard {
public:
  void init() {
    // We need the locking for enabling two Broker endpoints in one process.
    std::unique_lock guard{ssl_init_mtx_};
    if (ssl_lib_state == ssl_lib_state_t::uninitialized) {
      endpoint::init_ssl_api();
      ssl_lib_state = ssl_lib_state_t::initialized;
    }
  }

  ~ssl_lib_guard() {
    if (ssl_lib_state == ssl_lib_state_t::initialized) {
      endpoint::deinit_ssl_api();
      ssl_lib_state = ssl_lib_state_t::deinitialized;
    }
  }
};

// Note: just placing this into the run_impl body is not sufficient, because we
// pass OpenSSL context pointers to other system parts that may outlive the
// connector.
ssl_lib_guard global_ssl_guard;

} // namespace

void connector::run_impl(listener* sub, shared_filter_type* filter) {
  // Block SIGPIPE entirely on this thread.
  caf::net::multiplexer::block_sigpipe();
  using std::find_if;
  // When running with OpenSSL enabled, initialize the library.
  if (ssl_cfg_ != nullptr && !broker_cfg_.skip_ssl_init)
    global_ssl_guard.init();
  // Poll isn't terribly efficient nor fast, but the connector is not a
  // performance-critical system component. It only establishes connections and
  // reads handshake messages, so poll() is 'good enough' and we use it since
  // it's portable.
  connect_manager mgr{this_peer_, sub, filter, peer_statuses_.get(),
                      ssl_context_from_cfg(ssl_cfg_)};
  auto& fdset = mgr.fdset;
  fdset.push_back({pipe_rd_, read_mask, 0});
  bool done = false;
  pipe_reader prd{caf::net::pipe_socket{pipe_rd_}, &done};
  // Loop until we receive a shutdown via the pipe.
  while (!done) {
    int presult =
#ifdef CAF_WINDOWS
      ::WSAPoll(fdset.data(), static_cast<ULONG>(fdset.size()),
                mgr.next_timeout());
#else
      ::poll(fdset.data(), static_cast<nfds_t>(fdset.size()),
             mgr.next_timeout());
#endif
    if (presult < 0) {
      switch (caf::net::last_socket_error()) {
        case std::errc::interrupted:
          // nop
          break;
        default:
          log::network::error("poll-failed", "poll() failed");
          throw std::runtime_error("poll() failed");
      }
    } else if (presult > 0) {
      auto has_activity = [](auto& entry) { return entry.revents != 0; };
      auto i = std::find_if(fdset.begin(), fdset.end(), has_activity);
      BROKER_ASSERT(i != fdset.end());
      auto advance = [&i, &fdset, has_activity] {
        i = std::find_if(i + 1, fdset.end(), has_activity);
        return i != fdset.end();
      };
      do {
        if (i->fd == pipe_rd_) {
          if (i->revents & read_mask) {
            prd.read(mgr);
          } else if (i->revents & error_mask) {
            throw broken_pipe{i->revents};
          }
        } else {
          if (i->revents & read_mask) {
            mgr.continue_reading(*i);
          } else if (i->revents & write_mask) {
            mgr.continue_writing(*i);
          } else {
            mgr.abort(*i);
          }
          while ((i->revents & read_mask) && mgr.must_read_more(*i))
            mgr.continue_reading(*i);
        }
      } while (--presult > 0 && advance());
    }
    mgr.handle_timeouts();
    mgr.prepare_next_cycle();
  }
  log::network::debug("connector-done", "connector done");
}

} // namespace broker::internal
