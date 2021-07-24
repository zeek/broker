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
#include "broker/shutdown_options.hh"
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

  void peer_connected(const endpoint_id& peer_id) override {
    BROKER_TRACE(BROKER_ARG(peer_id));
    emit(peer_id, sc_constant<sc::peer_added>(), "handshake successful");
    super::peer_connected(peer_id);
  }

  void peer_disconnected(const endpoint_id& peer_id,
                         const error& reason) override {
    BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG(reason));
    network_info peer_addr;
    if (auto net_ptr = this->addr_of(peer_id))
      peer_addr = *net_ptr;
    emit(peer_id, peer_addr, sc_constant<sc::peer_lost>(),
         "lost connection to remote peer");
    super::peer_disconnected(peer_id, reason);
  }

  void peer_removed(const endpoint_id& peer_id) override {
    BROKER_TRACE(BROKER_ARG(peer_id));
    emit(peer_id, sc_constant<sc::peer_removed>(),
         "removed connection to remote peer");
    super::peer_removed(peer_id);
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

  void cannot_remove_peer(const network_info& x) override {
    cannot_remove_peer_impl(x);
  }

  void shutdown(shutdown_options options) override {
    // After calling shutdown, the flows may still push downstream whatever is
    // buffered at the moment but no new data can arrive. Push remaining status
    // updates to the buffer now to make sure they still arrive.
    if (!disable_notifications_) {
      for (auto& [peer_id, row] : super::tbl()) {
        if (!row.versioned_paths.empty()) {
          if (row.versioned_paths.front().first.size() == 1) {
            emit(peer_id, sc_constant<sc::peer_removed>(),
                 "removed connection to remote peer");
          }
          emit(peer_id, sc_constant<sc::endpoint_unreachable>(),
               "lost the last path");
        }
      }
      disable_notifications_ = true;
    }
    super::shutdown(std::move(options));
  }

  // -- initialization ---------------------------------------------------------

  caf::behavior make_behavior() override {
    return caf::message_handler{
      [this](atom::no_events) {
        BROKER_DEBUG("disable notifications");
        disable_notifications_ = true;
      },
      [this](atom::publish, endpoint_info& receiver, data_message& msg) {
        // TODO: implement me
        // this->ship(msg, receiver.node);
      },
      [](atom::add, atom::status, const caf::actor&) {
        // TODO: this handler exists only for backwards-compatibility. It used
        //       to register status subscribers for synchronization. Eventually,
        //       we should either re-implement the synchronization if necessary
        //       or remove this handler.
      },
    }
      .or_else(super::make_behavior());
  }

private:
  template <class T>
  void cannot_remove_peer_impl(const T& x) {
    BROKER_TRACE(BROKER_ARG(x));
    emit(x, ec_constant<ec::peer_invalid>(), "cannot unpeer from unknown peer");
    super::cannot_remove_peer(x);
  }

  void emit(const status& stat) {
    auto dmsg = make_data_message(topic::statuses(), get_as<data>(stat));
    this->publish_locally(std::move(dmsg));
  }

  void emit(const error& err) {
    auto dmsg = make_data_message(topic::errors(), get_as<data>(err));
    this->publish_locally(std::move(dmsg));
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

  template <class EnumConstant>
  void emit(const endpoint_id& peer_id, EnumConstant code, const char* msg) {
    BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG2("code", EnumConstant::value)
                                     << BROKER_ARG(msg));
    if (disable_notifications_)
      return;
    using value_type = typename EnumConstant::value_type;
    if constexpr (detail::has_network_info_v<EnumConstant>) {
      network_info net;
      if (auto net_ptr = this->addr_of(peer_id))
        net = *net_ptr;
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
