#pragma once

#include <caf/allowed_unsafe_message_type.hpp>
#include <caf/response_promise.hpp>

#include "broker/atoms.hh"
#include "broker/detail/lift.hh"
#include "broker/detail/network_cache.hh"
#include "broker/detail/retry_state.hh"
#include "broker/error.hh"
#include "broker/logger.hh"
#include "broker/message.hh"

namespace broker::mixin {

/// Adds these handlers:
///
/// ~~~
/// (atom::peer, network_info addr) -> void
/// => try_peering(addr, self->make_response_promise(), 0)
///
/// (atom::publish, network_info addr, data_message msg) -> void
/// => try_publish(addr, msg, self->make_response_promise())
/// ~~~
template <class Base, class Subtype>
class connector : public Base {
public:
  using extended_base = connector;

  using super = Base;

  using peer_id_type = typename super::peer_id_type;

  using communication_handle_type = typename Base::communication_handle_type;

  template <class... Ts>
  explicit connector(Ts&&... xs)
    : super(std::forward<Ts>(xs)...), cache_(super::self()) {
    // nop
  }

  void try_peering(const network_info& addr, caf::response_promise rp,
                   uint32_t count) {
    BROKER_TRACE(BROKER_ARG(count));
    auto self = super::self();
    // Fetch the comm. handle from the cache and with that fetch the ID from the
    // remote peer via direct request messages.
    cache_.fetch(
      addr,
      [=](communication_handle_type hdl) mutable {
        BROKER_DEBUG("lookup successful:" << BROKER_ARG(addr)
                                          << BROKER_ARG(hdl));
        dref().start_peering(hdl.node(), hdl, std::move(rp));
      },
      [=](error err) mutable {
        BROKER_DEBUG("lookup failed:" << BROKER_ARG(addr) << BROKER_ARG(err));
        dref().peer_unavailable(addr);
        if (addr.retry.count() == 0 && ++count < 10) {
          rp.deliver(std::move(err));
        } else {
          self->delayed_send(self, addr.retry,
                             detail::retry_state{addr, std::move(rp), count});
        }
      });
  }

  void try_publish(const network_info& addr, data_message& msg,
                   caf::response_promise rp) {
    auto self = super::self();
    cache_.fetch(
      addr,
      [=, msg{std::move(msg)}](communication_handle_type hdl) mutable {
        dref().ship(msg, hdl);
        rp.deliver(caf::unit);
      },
      [=](error err) mutable { rp.deliver(std::move(err)); });
  }

  void peer_disconnected(const peer_id_type& peer_id,
                         const communication_handle_type& hdl,
                         const error& reason) {
    if (!dref().shutting_down()) {
      auto x = cache_.find(hdl);
      if (x && x->retry != timeout::seconds(0)) {
        cache_.remove(hdl);
        BROKER_INFO("will try reconnecting to" << *x << "in"
                                               << to_string(x->retry));
        auto self = super::self();
        self->delayed_send(self, x->retry, atom::peer_v, atom::retry_v, *x);
      }
    }
    super::peer_disconnected(peer_id, hdl, reason);
  }

  template <class... Fs>
  caf::behavior make_behavior(Fs... fs) {
    using detail::lift;
    auto& d = dref();
    return super::make_behavior(
      std::move(fs)...,
      [=](atom::peer, const network_info& addr) {
        dref().try_peering(addr, super::self()->make_response_promise(), 0);
      },
      [=](atom::peer, atom::retry, network_info& addr) {
        dref().try_peering(addr, caf::response_promise{}, 0);
      },
      [=](detail::retry_state& st) {
        dref().try_peering(st.addr, std::move(st.rp), st.count);
      },
      [=](atom::publish, const network_info& addr, data_message& msg) {
        dref().try_publish(addr, msg, super::self()->make_response_promise());
      },
      [=](atom::unpeer, const network_info& addr) {
        if (auto hdl = cache_.find(addr))
          dref().unpeer(*hdl);
        else
          dref().cannot_remove_peer(addr);
      });
  }

  auto& cache() {
    return cache_;
  }

private:
  Subtype& dref() {
    return static_cast<Subtype&>(*this);
  }

  /// Associates network addresses to remote actor handles and vice versa.
  detail::network_cache cache_;
};

} // namespace broker::mixin
