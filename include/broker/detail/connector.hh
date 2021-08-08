#pragma once

#include <condition_variable>
#include <mutex>

#include <caf/actor.hpp>
#include <caf/net/pipe_socket.hpp>
#include <caf/net/stream_socket.hpp>
#include <caf/uuid.hpp>

#include "broker/detail/peer_status_map.hh"
#include "broker/fwd.hh"
#include "broker/network_info.hh"

namespace broker::detail {

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
                               network_info addr, alm::lamport_timestamp ts,
                               filter_type filter, caf::net::socket_id fd)
      = 0;

    virtual void on_redundant_connection(connector_event_id event_id,
                                         endpoint_id peer, network_info addr)
      = 0;

    virtual void
    on_drop(connector_event_id event_id, std::optional<endpoint_id> peer)
      = 0;

    virtual void on_listen(connector_event_id event_id, uint16_t port) = 0;

    virtual void on_error(connector_event_id event_id, error reason) = 0;

    virtual void on_shutdown() = 0;
  };

  explicit connector(endpoint_id this_peer);

  ~connector();

  void async_connect(connector_event_id event_id, const network_info& addr);

  void async_drop(connector_event_id event_id, const network_info& addr);

  void async_listen(connector_event_id event_id, const std::string& host,
                    uint16_t port);

  void async_shutdown();

  /// @thread-safe
  void init(std::unique_ptr<listener> sub, shared_filter_ptr filter,
            shared_peer_status_map_ptr peer_statuses);

  void run();

private:
  void run_impl(listener* sub, shared_filter_type* filter);

  void write_to_pipe(caf::span<const caf::byte> bytes);

  caf::net::pipe_socket pipe_wr_;
  caf::net::pipe_socket pipe_rd_;
  caf::uuid this_peer_;
  caf::actor worker_;
  std::mutex mtx_;
  std::condition_variable sub_cv_;
  std::unique_ptr<listener> sub_;
  shared_filter_ptr filter_;
  shared_peer_status_map_ptr peer_statuses_;
};

using connector_ptr = std::shared_ptr<connector>;

} // namespace broker::detail
