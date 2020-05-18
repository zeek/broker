#pragma once

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/behavior.hpp>
#include <caf/node_id.hpp>

#include "broker/alm/peer.hh"
#include "broker/alm/routing_table.hh"
#include "broker/detail/lift.hh"
#include "broker/filter_type.hh"
#include "broker/message.hh"

namespace broker::alm {

/// A transport based on asynchronous messages. For testing only.
template <class Derived, class PeerId>
class async_transport : public peer<Derived, PeerId, caf::actor> {
public:
  using super = peer<PeerId, caf::actor, Derived>;

  using peer_id_type = PeerId;

  void start_peering(const peer_id_type& remote_peer, caf::actor hdl) {
    BROKER_TRACE(BROKER_ARG(remote_peer) << BROKER_ARG(hdl));
    auto& d = dref();
    if (!d.tbl().emplace(std::move(remote_peer), std::move(hdl)).second) {
      BROKER_INFO("start_peering ignored: already peering with "
                  << remote_peer);
      return;
    }
    send(hdl, atom::peer::value, d.id(), d.filter(), d.timestamp());
  }

  /// Starts the handshake process for a new peering (step #1 in core_actor.cc).
  /// @param peer_hdl Handle to the peering (remote) core actor.
  /// @param peer_filter Filter of our peer.
  /// @param send_own_filter Sends a `(filter, self)` handshake if `true`,
  ///                        `('ok', self)` otherwise.
  /// @pre `current_sender() != nullptr`
  auto handle_peering(const peer_id_type& remote_id, const filter_type& filter,
                      lamport_timestamp timestamp) {
    BROKER_TRACE(BROKER_ARG(remote_id));
    // Check whether we already send outbound traffic to the peer. Could use
    // `BROKER_ASSERT` instead, because this mustn't get called for known peers.
    auto& d = dref();
    auto src = caf::actor_cast<caf::actor>(d.self()->current_sender());
    if (!d.tbl().emplace(remote_id, src).second)
      BROKER_INFO("received repeated peering request");
    // Propagate filter to peers.
    std::vector<peer_id_type> path{remote_id};
    vector_timestamp path_ts{timestamp};
    d.handle_filter_update(path, path_ts, filter);
    // Reply with our own filter.
    return caf::make_message(atom::peer::value, atom::ok::value, d.id(),
                             d.filter(), d.timestamp());
  }

  auto handle_peering_response(const peer_id_type& remote_id,
                               const filter_type& filter,
                               lamport_timestamp timestamp) {
    auto& d = dref();
    auto src = caf::actor_cast<caf::actor>(d.self()->current_sender());
    if (!d.tbl().emplace(remote_id, src).second)
      BROKER_INFO("received repeated peering response");
    // Propagate filter to peers.
    std::vector<peer_id_type> path{remote_id};
    vector_timestamp path_ts{timestamp};
    d.handle_filter_update(path, path_ts, filter);
  }

  template <class... Ts>
  void send(const caf::actor& receiver, Ts&&... xs) {
    dref().self()->send(receiver, std::forward<Ts>(xs)...);
  }

  template <class... Fs>
  caf::behavior make_behavior(Fs... fs) {
    using detail::lift;
    auto& d = dref();
    return {
      std::move(fs)...,
      lift<atom::peer>(d, &Derived::start_peering),
      lift<atom::peer>(d, &Derived::handle_peering),
      lift<atom::peer, atom::ok>(d, &Derived::handle_peering_response),
      lift<atom::publish>(d, &Derived::publish_data),
      lift<atom::publish>(d, &Derived::publish_command),
      lift<atom::subscribe>(d, &Derived::subscribe),
      lift<atom::publish>(d, &Derived::handle_publication),
      lift<atom::subscribe>(d, &Derived::handle_filter_update),
    };
  }

private:
  auto& dref() {
    return static_cast<Derived&>(*this);
  }
};

} // namespace broker::alm
