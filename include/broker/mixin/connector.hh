#pragma once

#include <caf/allowed_unsafe_message_type.hpp>
#include <caf/response_promise.hpp>

#include "broker/atoms.hh"
#include "broker/detail/lift.hh"
#include "broker/detail/network_cache.hh"
#include "broker/detail/retry_state.hh"
#include "broker/error.hh"
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
    auto self = super::self();
    // Fetch the comm. handle from the cache and with that fetch the ID from the
    // remote peer via direct request messages.
    cache_.fetch(
      addr,
      [=](communication_handle_type hdl) mutable {
        if (auto i = ids_.find(hdl); i != ids_.end()) {
          dref().start_peering(i->second, hdl, std::move(rp));
          return;
        }
        // TODO: replace infinite with some useful default / config parameter
        self->request(hdl, caf::infinite, atom::get_v, atom::id_v)
          .then(
            [=](const peer_id_type& remote_id) mutable {
              dref().start_peering(remote_id, hdl, std::move(rp));
            },
            [=](error& err) mutable { rp.deliver(std::move(err)); });
      },
      [=](error err) mutable {
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
    auto deliver_err = [=](error err) mutable { rp.deliver(std::move(err)); };
    cache_.fetch(
      addr,
      [=, msg{std::move(msg)}](communication_handle_type hdl) mutable {
        if (auto i = ids_.find(hdl); i != ids_.end()) {
          dref().ship(msg, i->second);
          rp.deliver(caf::unit);
          return;
        }
        // TODO: replace infinite with some useful default / config parameter
        self->request(hdl, caf::infinite, atom::get_v, atom::id_v)
          .then(
            [=, msg{std::move(msg)}](const peer_id_type& remote_id) mutable {
              ids_.emplace(hdl, remote_id);
              dref().ship(msg, remote_id);
              rp.deliver(caf::unit);
            },
            deliver_err);
      },
      deliver_err);
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
      [=](atom::publish, const network_info& addr, data_message& msg) {
        dref().try_publish(addr, msg, super::self()->make_response_promise());
      },
      [=](atom::unpeer, const network_info& addr) {
        if (auto hdl = cache_.find(addr))
          dref().unpeer(*hdl);
        else
          dref().cannot_remove_peer(addr);
      },
      [=](detail::retry_state& x) {
        dref().try_peering(x.addr, std::move(x.rp), x.count);
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

  /// Maps remote actor handles to peer IDs.
  std::unordered_map<caf::actor, peer_id_type> ids_;
};

} // namespace broker::mixin
