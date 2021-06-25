#pragma once

#include <functional>
#include <optional>
#include <unordered_map>

#include <caf/fwd.hpp>
#include <caf/net/fwd.hpp>

#include "broker/fwd.hh"

namespace broker::detail {

/// Connects an actor to a connector and provides a convenient callback
/// interface.
class connector_adapter {
public:
  template <class... Ts>
  using callback = std::function<void(Ts...)>;

  using peering_callback = callback<endpoint_id, caf::net::stream_socket>;

  using error_callback = callback<const caf::error&>;

  connector_adapter(caf::event_based_actor* self, connector_ptr conn,
                    peering_callback on_peering);

  caf::message_handler message_handlers();

  void async_connect(const network_info& addr,
                     callback<endpoint_id, caf::net::stream_socket> on_success,
                     error_callback on_error);

  void async_drop(const network_info& addr,
                  callback<std::optional<endpoint_id>> on_success,
                  error_callback on_error);

  void async_listen(const std::string& host, uint16_t port,
                    callback<uint16_t> on_success, error_callback on_error);

private:
  using msg_callback = callback<const caf::message&>;

  connector_event_id next_id();

  connector_ptr conn_;

  connector_event_id next_id_ = static_cast<connector_event_id>(1);

  peering_callback on_peering_;

  std::unordered_map<connector_event_id, msg_callback> pending_;
};

} // namespace broker::detail
