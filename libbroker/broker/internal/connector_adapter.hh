#pragma once

#include "broker/fwd.hh"
#include "broker/internal/connector.hh"

#include <caf/fwd.hpp>

#include <functional>
#include <optional>
#include <unordered_map>

namespace broker::internal {

/// Connects an actor to a connector and provides a convenient callback
/// interface.
class connector_adapter {
public:
  template <class... Ts>
  using callback = std::function<void(Ts...)>;

  /// Callback for incoming peering connections.
  using peering_callback = callback<endpoint_id, const network_info&,
                                    const filter_type&, pending_connection_ptr>;

  /// Callback for non-critical errors during peering. The connector emits those
  /// errors whenever a connection attempt has failed but retries afterwards
  /// after the configured delay. That the core may report to clients as
  /// `peer_unavailable` messages.
  using peer_unavailable_callback = callback<const network_info&>;

  using redundant_peering_callback = callback<endpoint_id, const network_info&>;

  using error_callback = callback<const caf::error&>;

  connector_adapter(caf::event_based_actor* self, connector_ptr conn,
                    peering_callback on_peering,
                    peer_unavailable_callback on_peer_unavailable,
                    shared_filter_ptr filter,
                    detail::shared_peer_status_map_ptr peer_statuses);

  caf::message_handler message_handlers();

  void async_connect(const network_info& addr, peering_callback on_success,
                     redundant_peering_callback on_redundant_peering,
                     error_callback on_error);

  void async_drop(const network_info& addr,
                  callback<std::optional<endpoint_id>> on_success,
                  error_callback on_error);

  void async_listen(const std::string& host, uint16_t port, bool reuse_addr,
                    callback<uint16_t> on_success, error_callback on_error);

  void async_shutdown();

private:
  using msg_callback = callback<const caf::message&>;

  connector_event_id next_id();

  connector_ptr conn_;

  connector_event_id next_id_ = static_cast<connector_event_id>(1);

  peering_callback on_peering_;

  peer_unavailable_callback on_peer_unavailable_;

  std::unordered_map<connector_event_id, msg_callback> pending_;
};

} // namespace broker::internal
