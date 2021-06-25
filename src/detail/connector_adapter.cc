#include "broker/detail/connector_adapter.hh"

#include <caf/const_typed_message_view.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/message.hpp>
#include <caf/message_handler.hpp>
#include <caf/net/socket_id.hpp>
#include <caf/net/stream_socket.hpp>
#include <caf/send.hpp>

#include "broker/detail/connector.hh"
#include "broker/logger.hh"

namespace broker::detail {

namespace {

template <class... Ts>
using msg_view = caf::const_typed_message_view<Ts...>;

class listener_impl : public connector::listener {
public:
  listener_impl(caf::actor hdl) : hdl_(std::move(hdl)) {
    // nop
  }

  void on_connection(endpoint_id peer, caf::net::socket_id fd) override {
    caf::anon_send(hdl_, invalid_connector_event_id,
                   caf::make_message(peer, fd));
  }

  void on_connection(connector_event_id event_id, endpoint_id peer,
                     caf::net::socket_id fd) override {
    caf::anon_send(hdl_, event_id, caf::make_message(peer, fd));
  }

  void on_drop(connector_event_id event_id,
               std::optional<endpoint_id> peer) override {
    caf::anon_send(hdl_, event_id, caf::make_message(peer));
  }

  void on_listen(connector_event_id event_id, uint16_t port) override {
    caf::anon_send(hdl_, event_id, caf::make_message(port));
  }

  void on_error(connector_event_id event_id, error reason) override {
    caf::anon_send(hdl_, event_id, caf::make_message(std::move(reason)));
  }

  void on_shutdown() override {
    caf::anon_send(hdl_, invalid_connector_event_id,
                   caf::make_message(atom::shutdown_v));
  }

private:
  caf::actor hdl_;
};

} // namespace

connector_adapter::connector_adapter(caf::event_based_actor* self,
                                     connector_ptr conn, peering_callback cb)
  : conn_(std::move(conn)), on_peering_(std::move(cb)) {
  conn_->init(std::make_unique<listener_impl>(caf::actor{self}));
}

connector_event_id connector_adapter::next_id() {
  auto result = next_id_;
  auto val = static_cast<uint64_t>(result);
  next_id_ = static_cast<connector_event_id>(val + 1);
  return result;
}

caf::message_handler connector_adapter::message_handlers() {
  using caf::get;
  return {
    [this](connector_event_id id, const caf::message& msg) {
      if (id == invalid_connector_event_id) {
        if (auto xs1 = msg_view<atom::shutdown>{msg}) {
          BROKER_DEBUG("lost the connector");
          // TODO: implement me
        } else if (auto xs2 = msg_view<endpoint_id, caf::net::socket_id>{msg}) {
          on_peering_(get<0>(xs2), caf::net::stream_socket{get<1>(xs2)});
        } else {
          BROKER_ERROR("connector_adapter received unexpected message:" << msg);
        }
      } else if (auto i = pending_.find(id); i != pending_.end()) {
        i->second(msg);
        pending_.erase(i);
      }
    },
  };
}

void connector_adapter::async_connect(
  const network_info& addr,
  callback<endpoint_id, caf::net::stream_socket> on_success,
  error_callback on_error) {
  using caf::get;
  using std::move;
  auto h = [f{move(on_success)}, g(move(on_error))](const caf::message& msg) {
    if (auto xs = msg_view<endpoint_id, caf::net::socket_id>(msg)) {
      f(get<0>(xs), caf::net::stream_socket{get<1>(xs)});
    } else if (auto ys = msg_view<caf::error>(msg)) {
      g(get<0>(ys));
    } else {
      auto err = caf::make_error(caf::sec::unexpected_message, msg);
      g(err);
    }
  };
  auto eid = next_id();
  pending_.emplace(eid, std::move(h));
  conn_->async_connect(eid, addr);
}

void connector_adapter::async_listen(const std::string& host, uint16_t port,
                                     callback<uint16_t> on_success,
                                     error_callback on_error) {
  using caf::get;
  using std::move;
  auto h = [f{move(on_success)}, g(move(on_error))](const caf::message& msg) {
    if (auto xs = msg_view<uint16_t>(msg)) {
      f(get<0>(xs));
    } else if (auto ys = msg_view<caf::error>(msg)) {
      g(get<0>(ys));
    } else {
      auto err = caf::make_error(caf::sec::unexpected_message, msg);
      g(err);
    }
  };
  auto eid = next_id();
  pending_.emplace(eid, std::move(h));
  conn_->async_listen(eid, host, port);
}

} // namespace broker::detail
