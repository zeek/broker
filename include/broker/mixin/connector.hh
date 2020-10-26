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
  // -- member types -----------------------------------------------------------

  using extended_base = connector;

  using super = Base;

  using peer_id_type = typename super::peer_id_type;

  using communication_handle_type = typename Base::communication_handle_type;

  // -- constructors, destructors, and assignment operators --------------------

  template <class... Ts>
  explicit connector(Ts&&... xs)
    : super(std::forward<Ts>(xs)...), cache_(super::self()) {
    // nop
  }

  // -- lazy connection management ---------------------------------------------

  void try_peering(const network_info& addr, caf::response_promise rp,
                   uint32_t count) {
    BROKER_TRACE(BROKER_ARG(addr) << BROKER_ARG(count));
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
        self->request(hdl, caf::infinite, atom::ping_v, dref().id(), self)
          .then(
            [=](atom::pong, const peer_id_type& remote_id,
                [[maybe_unused]] communication_handle_type hdl2) mutable {
              BROKER_ASSERT(hdl == hdl2);
              dref().start_peering(remote_id, hdl, std::move(rp));
            },
            [=](error& err) mutable { rp.deliver(std::move(err)); });
      },
      [=](error err) mutable {
        dref().peer_unavailable(addr);
        ++count; // Tracked, but currently unused; could implement max. count.
        if (addr.retry.count() == 0) {
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

  // -- factories --------------------------------------------------------------

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
      },
      [=](atom::ping, const peer_id_type& peer_id,
          const communication_handle_type& hdl) {
        // This step only exists to populate the network caches on both sides
        // before starting the actual handshake.
        auto rp = dref().self()->make_response_promise();
        cache_.fetch(
          hdl,
          [this, rp](const network_info&) mutable {
            rp.deliver(atom::pong_v, dref().id(), dref().self());
          },
          [rp](error err) mutable { rp.deliver(std::move(err)); });
        return rp;
      });
  }

  // -- callbacks --------------------------------------------------------------

  void peer_disconnected(const peer_id_type& peer_id,
                         const communication_handle_type& hdl,
                         const error& reason) {
    // Lost network connection: try reconnecting.
    BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG(hdl) << BROKER_ARG(reason));
    if (auto addr = cache_.find(hdl)) {
      // Drop any previous state and trigger a new connection cycle.
      ids_.erase(hdl);
      cache_.remove(hdl);
      // The naive thing to do here would be calling
      // `dref().try_peering(*addr, {}, 0)` to trigger the reconnect loop.
      // However, `peer_disconnected` is not necessarily triggered by a
      // disconnect. Broker endpoints tear down the peering relations as part of
      // a regular shutdown. Hence, we must somehow delay the reconnect attempts
      // until the connection actually ceased to exist.
      // TODO: when fully switching to CAF 0.18, we should use the new node
      //       monitoring and `node_down_msg` signaling instead for a cleaner
      //       and ultimately more robust implementation. Using attach on the
      //       actor handle assumes that the down message triggers after losing
      //       connection to the remote endpoint actor. That's not necessarily
      //       the case, though. We could still see the down message before CAF
      //       actually shuts down the connection. Shutting down the endpoint
      //       actor generally comes last before tearing down a Broker process,
      //       so adding the 250ms delay at least makes it very unlikely that
      //       the connection still exists. Still, this entire block is a hack
      //       and we should move on to node monitoring once we no longer care
      //       for CAF 0.17 compatibility.
      auto weak_self = dref().self()->address();
      hdl->attach_functor(
        [weak_self, addr{std::move(*addr)}](const caf::error& rsn) mutable {
          // Trigger reconnect after 250ms when still alive and kicking.
          if (auto strong_self = caf::actor_cast<caf::actor>(weak_self))
            caf::delayed_anon_send(caf::actor(strong_self),
                                   std::chrono::milliseconds{250}, atom::peer_v,
                                   std::move(addr));
        });
    }
    super::peer_disconnected(peer_id, hdl, reason);
  }

  void peer_removed(const peer_id_type& peer_id,
                    const communication_handle_type& hdl) {
    // Graceful removal by the user: remove all state associated to the peer.
    BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG(hdl));
    ids_.erase(hdl);
    cache_.remove(hdl);
    super::peer_removed(peer_id, hdl);
  }


  // -- properties -------------------------------------------------------------

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
  std::unordered_map<communication_handle_type, peer_id_type> ids_;
};

} // namespace broker::mixin
