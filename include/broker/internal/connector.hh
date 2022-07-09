#pragma once

#include "broker/configuration.hh"
#include "broker/detail/native_socket.hh"
#include "broker/detail/peer_status_map.hh"
#include "broker/fwd.hh"
#include "broker/internal/pending_connection.hh"
#include "broker/network_info.hh"

#include <caf/actor.hpp>
#include <caf/error.hpp>
#include <caf/net/openssl_transport.hpp>

#include <condition_variable>
#include <mutex>

namespace broker::internal {

// TODO: exposed for internal/web_socket.cc, drop when switching to CAF 0.19.
caf::net::openssl::ctx_ptr ssl_context_from_cfg(const openssl_options_ptr& cfg);

// "Strong typedef" akin to std::byte;
enum class connector_event_id : uint64_t {};

template <class Inspector>
bool inspect(Inspector& f, connector_event_id& x) {
  auto get = [&x] { return static_cast<uint64_t>(x); };
  auto set = [&x](uint64_t val) { x = static_cast<connector_event_id>(val); };
  return f.apply(get, set);
}

constexpr auto invalid_connector_event_id = connector_event_id{0};

constexpr bool valid(connector_event_id id) noexcept {
  return id != invalid_connector_event_id;
}

class connector {
public:
  class listener {
  public:
    virtual ~listener();

    /// Signals that a remote node has connected to this peer.
    virtual void on_connection(connector_event_id event_id, endpoint_id peer,
                               network_info addr, filter_type filter,
                               pending_connection_ptr ptr) = 0;

    virtual void on_redundant_connection(connector_event_id event_id,
                                         endpoint_id peer,
                                         network_info addr) = 0;

    virtual void on_drop(connector_event_id event_id,
                         std::optional<endpoint_id> peer) = 0;

    virtual void on_listen(connector_event_id event_id, uint16_t port) = 0;

    virtual void on_error(connector_event_id event_id, caf::error reason) = 0;

    virtual void on_peer_unavailable(const network_info&) = 0;

    virtual void on_shutdown() = 0;
  };

  connector(endpoint_id this_peer, broker_options broker_cfg,
            openssl_options_ptr ssl_cfg);

  ~connector();

  void async_connect(connector_event_id event_id, const network_info& addr);

  void async_drop(connector_event_id event_id, const network_info& addr);

  void async_listen(connector_event_id event_id, const std::string& host,
                    uint16_t port, bool reuse_addr);

  void async_shutdown();

  /// @thread-safe
  void init(std::unique_ptr<listener> sub, shared_filter_ptr filter,
            detail::shared_peer_status_map_ptr peer_statuses);

  void run();

private:
  void run_impl(listener* sub, shared_filter_type* filter);

  void write_to_pipe(caf::span<const caf::byte> bytes,
                     bool shutdown_after_write = false);

  std::mutex mtx_;
  std::condition_variable sub_cv_;
  bool shutting_down_ = false;
  detail::native_socket pipe_wr_;
  detail::native_socket pipe_rd_;
  endpoint_id this_peer_;
  caf::actor worker_;
  std::unique_ptr<listener> sub_;
  shared_filter_ptr filter_;
  detail::shared_peer_status_map_ptr peer_statuses_;
  broker_options broker_cfg_;
  openssl_options_ptr ssl_cfg_;
};

using connector_ptr = std::shared_ptr<connector>;

} // namespace broker::internal
