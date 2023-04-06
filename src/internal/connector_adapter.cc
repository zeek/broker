#include "broker/internal/connector_adapter.hh"

#include <caf/const_typed_message_view.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/message.hpp>
#include <caf/message_handler.hpp>
#include <caf/send.hpp>

#include "broker/detail/peer_status_map.hh"
#include "broker/filter_type.hh"
#include "broker/internal/connector.hh"
#include "broker/internal/logger.hh"
#include "broker/internal/type_id.hh"

namespace broker::internal {

namespace {

class listener_impl : public connector::listener {
public:
  listener_impl(caf::actor hdl) : hdl_(std::move(hdl)) {
    // nop
  }

  void on_connection(connector_event_id event_id, endpoint_id peer,
                     network_info addr, filter_type filter,
                     pending_connection_ptr ptr) override {
    BROKER_TRACE(BROKER_ARG(event_id)
                 << BROKER_ARG(peer) << BROKER_ARG(addr) << BROKER_ARG(filter));
    caf::anon_send(hdl_, event_id,
                   caf::make_message(peer, addr, std::move(filter),
                                     std::move(ptr)));
  }

  void on_redundant_connection(connector_event_id event_id, endpoint_id peer,
                               network_info addr) override {
    BROKER_TRACE(BROKER_ARG(event_id) << BROKER_ARG(peer) << BROKER_ARG(addr));
    caf::anon_send(hdl_, event_id, caf::make_message(peer, addr));
  }

  void on_drop(connector_event_id event_id,
               std::optional<endpoint_id> peer) override {
    BROKER_TRACE(BROKER_ARG(event_id) << BROKER_ARG(peer));
    caf::anon_send(hdl_, event_id, caf::make_message(peer));
  }

  void on_listen(connector_event_id event_id, uint16_t port) override {
    BROKER_TRACE(BROKER_ARG(event_id) << BROKER_ARG(port));
    caf::anon_send(hdl_, event_id, caf::make_message(port));
  }

  void on_error(connector_event_id event_id, caf::error reason) override {
    BROKER_TRACE(BROKER_ARG(event_id) << BROKER_ARG(reason));
    caf::anon_send(hdl_, event_id, caf::make_message(std::move(reason)));
  }

  void on_peer_unavailable(const network_info& addr) override {
    BROKER_TRACE(BROKER_ARG(addr));
    caf::anon_send(hdl_, invalid_connector_event_id, caf::make_message(addr));
  }

  void on_shutdown() override {
    BROKER_TRACE("");
    caf::anon_send(hdl_, invalid_connector_event_id,
                   caf::make_message(atom::shutdown_v));
  }

private:
  caf::actor hdl_;
};

auto connection_event(const caf::message& msg) {
  return caf::make_const_typed_message_view<
    endpoint_id, network_info, filter_type, pending_connection_ptr>(msg);
}

auto redundant_connection_event(const caf::message& msg) {
  return caf::make_const_typed_message_view<endpoint_id, network_info>(msg);
}

auto listen_event(const caf::message& msg) {
  return caf::make_const_typed_message_view<uint16_t>(msg);
}

auto error_event(const caf::message& msg) {
  return caf::make_const_typed_message_view<caf::error>(msg);
}

auto peer_unavailable_event(const caf::message& msg) {
  return caf::make_const_typed_message_view<network_info>(msg);
}

auto shutdown_event(const caf::message& msg) {
  return caf::make_const_typed_message_view<atom::shutdown>(msg);
}

} // namespace

connector_adapter::connector_adapter(caf::event_based_actor* self,
                                     connector_ptr conn, peering_callback cb,
                                     peer_unavailable_callback on_unavailable,
                                     shared_filter_ptr filter,
                                     detail::shared_peer_status_map_ptr ps_map)
  : conn_(std::move(conn)),
    on_peering_(std::move(cb)),
    on_peer_unavailable_(std::move(on_unavailable)) {
  conn_->init(std::make_unique<listener_impl>(caf::actor{self}),
              std::move(filter), std::move(ps_map));
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
    [this](connector_event_id event_id, const caf::message& msg) {
      BROKER_TRACE(BROKER_ARG(event_id) << BROKER_ARG(msg));
      if (event_id == invalid_connector_event_id) {
        if (auto xs1 = shutdown_event(msg)) {
          BROKER_DEBUG("lost the connector");
          // TODO: implement me? Anything we could do here?
        } else if (auto xs2 = connection_event(msg)) {
          on_peering_(get<0>(xs2), get<1>(xs2), get<2>(xs2), get<3>(xs2));
        } else if (auto xs3 = redundant_connection_event(msg)) {
          // drop
        } else if (auto xs4 = peer_unavailable_event(msg)) {
          on_peer_unavailable_(get<0>(xs4));
        } else {
          BROKER_ERROR("connector_adapter received unexpected message:" << msg);
        }
      } else if (auto i = pending_.find(event_id); i != pending_.end()) {
        i->second(msg);
        pending_.erase(i);
      }
    },
  };
}

void connector_adapter::async_connect(const network_info& addr,
                                      peering_callback f,
                                      redundant_peering_callback g,
                                      error_callback h) {
  BROKER_TRACE(BROKER_ARG(addr));
  using caf::get;
  auto cb = [f{std::move(f)}, g{std::move(g)},
             h{std::move(h)}](const caf::message& msg) {
    if (auto xs1 = connection_event(msg)) {
      f(get<0>(xs1), get<1>(xs1), get<2>(xs1), get<3>(xs1));
    } else if (auto xs2 = redundant_connection_event(msg)) {
      g(get<0>(xs2), get<1>(xs2));
    } else if (auto xs3 = error_event(msg)) {
      h(get<0>(xs3));
    } else {
      auto err = caf::make_error(caf::sec::unexpected_message, msg);
      h(err);
    }
  };
  auto eid = next_id();
  pending_.emplace(eid, std::move(cb));
  conn_->async_connect(eid, addr);
}

void connector_adapter::async_listen(const std::string& host, uint16_t port,
                                     bool reuse_addr,
                                     callback<uint16_t> on_success,
                                     error_callback on_error) {
  BROKER_TRACE(BROKER_ARG(host) << BROKER_ARG(port) << BROKER_ARG(reuse_addr));
  using caf::get;
  auto h = [f{std::move(on_success)},
            g(std::move(on_error))](const caf::message& msg) {
    if (auto xs = listen_event(msg)) {
      f(get<0>(xs));
    } else if (auto ys = error_event(msg)) {
      g(get<0>(ys));
    } else {
      auto err = caf::make_error(caf::sec::unexpected_message, msg);
      g(err);
    }
  };
  auto eid = next_id();
  pending_.emplace(eid, std::move(h));
  conn_->async_listen(eid, host, port, reuse_addr);
}

void connector_adapter::async_shutdown() {
  conn_->async_shutdown();
}

} // namespace broker::internal
