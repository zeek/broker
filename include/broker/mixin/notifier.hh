#pragma once

#include <caf/behavior.hpp>
#include <caf/group.hpp>

#include "broker/atoms.hh"
#include "broker/data.hh"
#include "broker/detail/assert.hh"
#include "broker/detail/has_network_info.hh"
#include "broker/detail/lift.hh"
#include "broker/endpoint_info.hh"
#include "broker/error.hh"
#include "broker/logger.hh"
#include "broker/message.hh"
#include "broker/status.hh"
#include "broker/topic.hh"

namespace broker::mixin {

template <class Base>
class notifier : public Base {
public:
  // -- member types -----------------------------------------------------------

  using super = Base;

  using extended_base = notifier;

  // -- constructors, destructors, and assignment operators --------------------

  template <class... Ts>
  explicit notifier(caf::event_based_actor* self, Ts&&... xs)
    : super(self, std::forward<Ts>(xs)...) {
    // nop
  }

  notifier() = delete;

  notifier(const notifier&) = delete;

  notifier& operator=(const notifier&) = delete;

  // -- overrides --------------------------------------------------------------

  void peer_discovered(const endpoint_id& peer_id) override {
    BROKER_TRACE(BROKER_ARG(peer_id));
    emit(peer_id, sc_constant<sc::endpoint_discovered>(),
         "found a new peer in the network");
    super::peer_discovered(peer_id);
  }

  void peer_connected(const endpoint_id& peer_id,
                      const caf::actor& hdl) override {
    BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG(hdl));
    emit(peer_id, sc_constant<sc::peer_added>(), "handshake successful");
    super::peer_connected(peer_id, hdl);
  }

  void peer_disconnected(const endpoint_id& peer_id, const caf::actor& hdl,
                         const error& reason) override {
    BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG(hdl) << BROKER_ARG(reason));
    // Calling emit() with the peer_id only trigges a network info lookup that
    // can stall this actor if we're already in shutdown mode. Hence, we perform
    // a manual cache lookup and simply omit the network information if we
    // cannot find a cached entry.
    network_info peer_addr;
    if (auto addr = this->cache().find(hdl))
      peer_addr = *addr;
    emit(peer_id, peer_addr, sc_constant<sc::peer_lost>(),
         "lost connection to remote peer");
    super::peer_disconnected(peer_id, hdl, reason);
  }

  void peer_removed(const endpoint_id& peer_id,
                    const caf::actor& hdl) override {
    BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG(hdl));
    emit(peer_id, sc_constant<sc::peer_removed>(),
         "removed connection to remote peer");
    super::peer_removed(peer_id, hdl);
  }

  void peer_unreachable(const endpoint_id& peer_id) override {
    BROKER_TRACE(BROKER_ARG(peer_id));
    emit(peer_id, sc_constant<sc::endpoint_unreachable>(),
         "lost the last path");
    super::peer_unreachable(peer_id);
  }

  void peer_unavailable(const network_info& addr) override {
    BROKER_TRACE(BROKER_ARG(addr));
    auto self = super::self();
    emit({}, addr, ec_constant<ec::peer_unavailable>(),
         "unable to connect to remote peer");
  }

  void cannot_remove_peer(const endpoint_id& x) override {
    cannot_remove_peer_impl(x);
  }

  void cannot_remove_peer(const caf::actor& x) override {
    cannot_remove_peer_impl(x);
  }

  void cannot_remove_peer(const network_info& x) override {
    cannot_remove_peer_impl(x);
  }

  // -- initialization ---------------------------------------------------------

  template <class... Fs>
  caf::behavior make_behavior(Fs... fs) {
    using detail::lift;
    return super::make_behavior(
      fs...,
      [this](atom::no_events) {
        BROKER_DEBUG("disabled notifications");
        disable_notifications_ = true;
      },
      [=](atom::publish, endpoint_info& receiver, data_message& msg) {
        this->ship(msg, receiver.node);
      },
      [](atom::add, atom::status, const caf::actor&) {
        // TODO: this handler exists only for backwards-compatibility. It used
        //       to register status subscribers for synchronization. Eventually,
        //       we should either re-implement the synchronization if necessary
        //       or remove this handler.
      });
  }

private:
  template <class T>
  void cannot_remove_peer_impl(const T& x) {
    BROKER_TRACE(BROKER_ARG(x));
    emit(x, ec_constant<ec::peer_invalid>(), "cannot unpeer from unknown peer");
    super::cannot_remove_peer(x);
  }

  void emit(const status& stat) {
    auto dmsg = make_data_message(topics::statuses, get_as<data>(stat));
    this->ship_locally(std::move(dmsg));
  }

  void emit(const error& err) {
    auto dmsg = make_data_message(topics::errors, get_as<data>(err));
    this->ship_locally(std::move(dmsg));
  }

  template <class Enum, Enum Code>
  void emit(const endpoint_id& peer_id, const network_info& x,
            std::integral_constant<Enum, Code>, const char* msg) {
    BROKER_INFO("emit:" << Code << x);
    if (disable_notifications_)
      return;
    if constexpr (std::is_same<Enum, sc>::value)
      emit(status::make<Code>(endpoint_info{peer_id, x}, msg));
    else
      emit(make_error(Code, endpoint_info{peer_id, x}, msg));
  }

  template <class Enum, Enum Code>
  void emit(const network_info& x, std::integral_constant<Enum, Code>,
            const char* msg) {
    BROKER_INFO("emit:" << Code << x);
    if (disable_notifications_)
      return;
    if constexpr (std::is_same<Enum, sc>::value) {
      emit(status::make<Code>(endpoint_info{{}, x}, msg));
    } else {
      emit(make_error(Code, endpoint_info{{}, x}, msg));
    }
  }

  /// Reports an error to all status subscribers.
  template <class EnumConstant>
  void emit(const caf::actor& hdl, EnumConstant code, const char* msg) {
    static_assert(detail::has_network_info_v<EnumConstant>);
    if (disable_notifications_)
      return;
    auto unbox_or_default = [](auto maybe_value) {
      using value_type = std::decay_t<decltype(*maybe_value)>;
      if (maybe_value)
        return std::move(*maybe_value);
      return value_type{};
    };
    emit(unbox_or_default(get_peer_id(this->tbl(), hdl)),
         unbox_or_default(this->cache().find(hdl)), code, msg);
  }

  template <class EnumConstant>
  void emit(const endpoint_id& peer_id, EnumConstant code, const char* msg) {
    BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG2("code", EnumConstant::value)
                                     << BROKER_ARG(msg));
    if (disable_notifications_)
      return;
    using value_type = typename EnumConstant::value_type;
    if constexpr (detail::has_network_info_v<EnumConstant>) {
      network_info net;
      auto& tbl = this->tbl();
      if (auto i = tbl.find(peer_id); i != tbl.end() && i->second.hdl)
        if (auto maybe_net = this->cache().find(i->second.hdl))
          net = std::move(*maybe_net);
       emit(peer_id, net, code, msg);
    } else if constexpr (std::is_same<value_type, sc>::value) {
      emit(status::make<EnumConstant::value>(peer_id, msg));
    } else {
      emit(make_error(EnumConstant::value, endpoint_info{peer_id, nil}, msg));
    }
  }

  bool disable_notifications_ = false;
};

} // namespace broker::mixin
