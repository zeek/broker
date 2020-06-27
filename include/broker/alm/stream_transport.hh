#pragma once

#include <vector>

#include <caf/broadcast_downstream_manager.hpp>
#include <caf/cow_tuple.hpp>
#include <caf/detail/scope_guard.hpp>
#include <caf/detail/unordered_flat_map.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/fused_downstream_manager.hpp>

#include "broker/alm/peer.hh"
#include "broker/alm/routing_table.hh"
#include "broker/detail/lift.hh"
#include "broker/detail/prefix_matcher.hh"
#include "broker/error.hh"
#include "broker/filter_type.hh"
#include "broker/message.hh"

namespace broker::alm {

/// The transport registers these message handlers:
///
/// ~~~
/// (atom::peer, peer_id_type id, actor hdl) -> void
/// => start_peering(id, hdl)
///
/// (atom::peer, actor, peer_id_type, filter_type, lamport_timestamp) -> slot
/// => handle_peering_request(...)
///
/// (stream<node_message>, actor, peer_id_type, filter_type, lamport_timestamp) -> slot
/// => handle_peering_handshake_1(...)
///
/// (stream<node_message>, actor, peer_id_type) -> void
/// => handle_peering_handshake_2(...)
///
/// (atom::unpeer, actor hdl) -> void
/// => disconnect(hdl)
/// ~~~
template <class Derived, class PeerId>
class stream_transport : public peer<Derived, PeerId, caf::actor>,
                         public caf::stream_manager {
public:
  // -- member types -----------------------------------------------------------

  using super = peer<Derived, PeerId, caf::actor>;

  using peer_id_type = PeerId;

  using message_type = typename super::message_type;

  struct pending_connection {
    caf::actor hdl;
    std::vector<caf::response_promise> promises;
  };

  /// Helper trait for defining streaming-related types for local actors
  /// (workers and stores).
  template <class T>
  struct local_trait {
    /// Type of a single element in the stream.
    using element = caf::cow_tuple<topic, T>;

    /// Type of a full batch in the stream.
    using batch = std::vector<element>;

    /// Type of the downstream_manager that broadcasts data to local actors.
    using manager = caf::broadcast_downstream_manager<element, filter_type,
                                                      detail::prefix_matcher>;
  };

  /// Streaming-related types for workers.
  using worker_trait = local_trait<data>;

  /// Streaming-related types for stores.
  using store_trait = local_trait<internal_command>;

  /// Streaming-related types for sources that produce both types of messages.
  struct var_trait {
    using batch = std::vector<node_message_content>;
  };

  /// Streaming-related types for peers.
  struct peer_trait {
    /// Type of a single element in the stream.
    using element = message_type;

    using batch = std::vector<element>;

    /// Type of the downstream_manager that broadcasts data to local actors.
    using manager = caf::broadcast_downstream_manager<element>;
  };

  /// Composed downstream_manager type for bundled dispatching.
  using downstream_manager_type
    = caf::fused_downstream_manager<typename peer_trait::manager,
                                    typename worker_trait::manager,
                                    typename store_trait::manager>;

  /// Maps a peer handle to the slot for outbound communication. Our routing
  /// table translates peer IDs to actor handles, but we need one additional
  /// step to get to the associated stream.
  using hdl_to_slot_map
    = caf::detail::unordered_flat_map<caf::actor, caf::stream_slot>;

  // -- constructors, destructors, and assignment operators --------------------

  explicit stream_transport(caf::event_based_actor* self)
    : super(self), caf::stream_manager(self), out_(this) {
    continuous(true);
  }

  // -- properties -------------------------------------------------------------

  auto tbl() noexcept -> decltype(super::tbl()) {
    return super::tbl();
  }

  caf::event_based_actor* self() noexcept {
    // Our only constructor accepts an event-based actor. Hence, we know for
    // sure that this case is safe, even though the base type stores self_ as a
    // scheduled_actor pointer.
    return static_cast<caf::event_based_actor*>(this->self_);
  }

  /// Returns the slot for outgoing traffic to `hdl`.
  optional<caf::stream_slot> output_slot(const caf::actor& hdl) const noexcept {
    auto i = hdl_to_ostream_.find(hdl);
    if (i == hdl_to_ostream_.end())
      return nil;
    return i->second;
  }

  /// Returns the slot for incoming traffic from `hdl`.
  optional<caf::stream_slot> input_slot(const caf::actor& hdl) const noexcept {
    auto i = hdl_to_istream_.find(hdl);
    if (i == hdl_to_istream_.end())
      return nil;
    return i->second;
  }

  /// Returns whether this manager has inbound and outbound streams from and to
  /// `hdl`.`
  bool connected_to(const caf::actor& hdl) const noexcept {
    return output_slot(hdl) && input_slot(hdl);
  }

  const auto& pending_connections() const noexcept {
    return pending_connections_;
  }

  auto& peer_manager() noexcept {
    return out_.template get<typename peer_trait::manager>();
  }

  auto& worker_manager() noexcept {
    return out_.template get<typename worker_trait::manager>();
  }

  auto& store_manager() noexcept {
    return out_.template get<typename store_trait::manager>();
  }

  // -- adding local subscribers -----------------------------------------------

  /// Subscribes `self->current_sender()` to `worker_manager()`.
  auto add_sending_worker(const filter_type& filter) {
    using element_type = typename worker_trait::element;
    using result_type = caf::outbound_stream_slot<element_type>;
    auto slot = add_unchecked_outbound_path<element_type>();
    if (slot != caf::invalid_stream_slot) {
      dref().subscribe(filter);
      out_.template assign<typename worker_trait::manager>(slot);
      worker_manager().set_filter(slot, filter);
    }
    return result_type{slot};
  }

  /// Subscribes `hdl` to `worker_manager()`.
  caf::error add_worker(const caf::actor& hdl, const filter_type& filter) {
    using element_type = typename worker_trait::element;
    auto slot = add_unchecked_outbound_path<element_type>(hdl);
    if (slot == caf::invalid_stream_slot)
      return caf::sec::cannot_add_downstream;
    dref().subscribe(filter);
    out_.template assign<typename worker_trait::manager>(slot);
    worker_manager().set_filter(slot, filter);
    return caf::none;
  }

  /// Subscribes `self->sender()` to `store_manager()`.
  auto add_sending_store(const filter_type& filter) {
    using element_type = typename store_trait::element;
    using result_type = caf::outbound_stream_slot<element_type>;
    auto slot = add_unchecked_outbound_path<element_type>();
    if (slot != caf::invalid_stream_slot) {
      dref().subscribe(filter);
      out_.template assign<typename store_trait::manager>(slot);
      store_manager().set_filter(slot, filter);
    }
    return result_type{slot};
  }

  /// Subscribes `hdl` to `store_manager()`.
  caf::error add_store(const caf::actor& hdl, const filter_type& filter) {
    using element_type = typename store_trait::element;
    auto slot = add_unchecked_outbound_path<element_type>(hdl);
    if (slot == caf::invalid_stream_slot)
      return caf::sec::cannot_add_downstream;
    dref().subscribe(filter);
    out_.template assign<typename store_trait::manager>(slot);
    store_manager().set_filter(slot, filter);
    return caf::none;
  }

  // -- publish and subscribe functions ----------------------------------------

  void publish_locally(data_message& msg) {
    worker_manager().push(msg);
  }

  void publish_locally(command_message& msg) {
    store_manager().push(msg);
  }

  // -- sending ----------------------------------------------------------------

  void stream_send(const caf::actor& receiver, message_type& msg) {
    // Fetch the output slot for reaching the receiver.
    auto i = hdl_to_ostream_.find(receiver);
    if (i == hdl_to_ostream_.end()) {
      BROKER_WARNING("unable to locate output slot for receiver");
      return;
    }
    auto slot = i->second;
    // Fetch the buffer for that slot and enqueue the message.
    auto& nested = out_.template get<typename peer_trait::manager>();
    auto j = nested.states().find(slot);
    if (j == nested.states().end()) {
      BROKER_WARNING("unable to access state for output slot");
      return;
    }
    j->second.buf.emplace_back(std::move(msg));
  }

  /// Sends an asynchronous message instead of pushing the data to the stream.
  /// Required for initiating handshakes (because no stream exists at that
  /// point) or for any other communication that bypasses the stream.
  template <class... Ts>
  void async_send(const caf::actor& receiver, Ts&&... xs) {
    self()->send(receiver, std::forward<Ts>(xs)...);
  }

  // Subscriptions use flooding.
  template <class... Ts>
  void send(const caf::actor& receiver, atom::subscribe atm, Ts&&... xs) {
    dref().async_send(receiver, atm, std::forward<Ts>(xs)...);
  }

  // Path revocations use flooding.
  template <class... Ts>
  void send(const caf::actor& receiver, atom::revoke atm, Ts&&... xs) {
    dref().async_send(receiver, atm, std::forward<Ts>(xs)...);
  }

  // Published messages use the stream.
  template <class T>
  void send(const caf::actor& receiver, atom::publish, T msg) {
    dref().stream_send(receiver, msg);
  }

  // -- peering ----------------------------------------------------------------

  // Initiates peering between A (this node) and B (remote peer).
  void start_peering(const peer_id_type& remote_peer, const caf::actor& hdl,
                     caf::response_promise rp) {
    BROKER_TRACE(BROKER_ARG(remote_peer) << BROKER_ARG(hdl));
    auto& d = dref();
    // We avoid conflicts in the handshake process by always having the node
    // with the smaller ID initiate the peering. Otherwise, we could end up in a
    // deadlock during handshake if both sides send step 1 at the sime time.
    if (remote_peer < d.id()) {
      self()
        ->request(hdl, std::chrono::minutes(10), atom::peer::value, d.id(),
                  self())
        .then(
          [=](atom::peer, atom::ok, const peer_id_type& peered_id) mutable {
            if (dref().id() != peered_id) {
              BROKER_ERROR("received peering response for unknown peer");
              rp.deliver(make_error(ec::peer_invalid, "unexpected peer ID",
                                    peered_id, remote_peer));
              return;
            }
            rp.deliver(atom::peer::value, atom::ok::value, remote_peer);
          },
          [=](caf::error& err) mutable { rp.deliver(std::move(err)); });
      return;
    }
    if (direct_connection(tbl(), remote_peer)) {
      BROKER_INFO("start_peering ignored: already peering with "
                  << remote_peer);
      return;
    }
    auto i = pending_connections_.find(remote_peer);
    if (i != pending_connections_.end()) {
      BROKER_DEBUG("already started peering to " << remote_peer);
      i->second.promises.emplace_back(self()->make_response_promise());
      return;
    }
    auto& pending = pending_connections_[remote_peer];
    pending.hdl = hdl;
    pending.promises.emplace_back(std::move(rp));
    self()->send(hdl, atom::peer::value, self(), d.id());
  }

  // Establishes a stream from B to A.
  caf::outbound_stream_slot<message_type, caf::actor, peer_id_type, filter_type,
                            lamport_timestamp>
  handle_peering_request(const caf::actor& hdl, const peer_id_type& peer_id) {
    BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(peer_id));
    auto& d = dref();
    // Sanity checking.
    if (!hdl) {
      BROKER_WARNING("received peering handshake with invalid handle");
      return {};
    }
    // Check whether there's already a peering relation established or underway.
    if (direct_connection(tbl(), peer_id)) {
      BROKER_DEBUG("drop peering request: already have a direct connection to"
                   << peer_id);
      return {};
    }
    if (hdl_to_ostream_.count(hdl) > 0 || hdl_to_istream_.count(hdl) > 0) {
      BROKER_DEBUG("drop peering request: already peering to " << peer_id);
    }
    // Open output stream (triggers handle_peering_handshake_1 on the remote),
    // sending our subscriptions, timestamp, etc. as handshake data.
    auto data = std::make_tuple(caf::actor_cast<caf::actor>(self()), d.id(),
                                d.filter(), d.timestamp());
    auto slot = d.template add_unchecked_outbound_path<message_type>(
      hdl, std::move(data));
    out_.template assign<typename peer_trait::manager>(slot);
    hdl_to_ostream_[hdl] = slot;
    return slot;
  }

  // Acks the stream from B to A and establishes a stream from A to B.
  caf::outbound_stream_slot<message_type, atom::ok, caf::actor, peer_id_type,
                            filter_type, lamport_timestamp>
  handle_peering_handshake_1(caf::stream<message_type> in,
                             const caf::actor& hdl, const peer_id_type& peer_id,
                             const filter_type& filter,
                             lamport_timestamp timestamp) {
    BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(peer_id) << BROKER_ARG(filter)
                                 << BROKER_ARG(timestamp));
    auto& d = dref();
    // Sanity checking. At this stage, we must have no direct connection routing
    // table entry and no open streams yet.
    if (!hdl) {
      BROKER_WARNING("received peering handshake with invalid handle");
      return {};
    }
    if (hdl_to_ostream_.count(hdl) != 0 || hdl_to_istream_.count(hdl) != 0) {
      BROKER_ERROR("drop handshake #1: already have open streams to"
                   << peer_id);
      return {};
    }
    // Trigger discovery event if this peer is new.
    auto trigger_peer_discovered = !reachable(tbl(), peer_id);
    // Add routing table entry for this direct connection.
    if (direct_connection(tbl(), peer_id)) {
      BROKER_ERROR("drop handshake #1: already have a direct connection to"
                   << peer_id);
      return {};
    }
    tbl()[peer_id].hdl = hdl;
    // Store filter and announce the new path.
    std::vector<peer_id_type> path{peer_id};
    vector_timestamp path_ts{timestamp};
    d.handle_filter_update(path, path_ts, filter);
    // Add streaming slots for this connection.
    auto data = std::make_tuple(atom::ok::value,
                                caf::actor_cast<caf::actor>(self()), d.id(),
                                d.filter(), d.timestamp());
    auto oslot = d.template add_unchecked_outbound_path<message_type>(
      hdl, std::move(data));
    out_.template assign<typename peer_trait::manager>(oslot);
    auto islot = d.add_unchecked_inbound_path(in);
    hdl_to_ostream_[hdl] = oslot;
    hdl_to_istream_[hdl] = islot;
    if (trigger_peer_discovered)
      d.peer_discovered(peer_id);
    d.peer_connected(peer_id, hdl);
    return oslot;
  }

  // Acks the stream from A to B.
  void handle_peering_handshake_2(caf::stream<message_type> in, atom::ok,
                                  const caf::actor& hdl,
                                  const peer_id_type& peer_id,
                                  const filter_type& topics,
                                  lamport_timestamp timestamp) {
    BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(peer_id));
    auto& d = dref();
    // Sanity checking. At this stage, we must have an open output stream but no
    // input stream yet.
    if (!hdl) {
      BROKER_WARNING("received peering handshake with invalid handle");
      return;
    }
    if (hdl_to_ostream_.count(hdl) == 0) {
      BROKER_ERROR("drop handshake #2: no open output stream to " << peer_id);
      return;
    }
    if (hdl_to_istream_.count(hdl) != 0) {
      BROKER_ERROR("drop handshake #2: already have inbound stream from"
                   << peer_id);
      return;
    }
    // Trigger discovery event if this peer is new.
    auto trigger_peer_discovered = !reachable(tbl(), peer_id);
    // Add routing table entry for this direct connection.
    if (direct_connection(tbl(), peer_id)) {
      BROKER_ERROR("drop handshake #1: already have a direct connection to"
                   << peer_id);
      return;
    }
    tbl()[peer_id].hdl = hdl;
    if (trigger_peer_discovered)
      d.peer_discovered(peer_id);
    d.peer_connected(peer_id, hdl);
    // Add inbound streaming slot for this connection.
    hdl_to_istream_[hdl] = d.add_unchecked_inbound_path(in);
    // Store subscriptions and announce the new path.
    std::vector<peer_id_type> path{peer_id};
    vector_timestamp path_ts{timestamp};
    d.handle_filter_update(path, path_ts, topics);
  }

  // -- callbacks --------------------------------------------------------------

  /// Pushes `msg` to local workers.
  void ship_locally(const data_message& msg) {
    if (!worker_manager().paths().empty())
      worker_manager().push(msg);
    super::ship_locally(msg);
  }

  /// Pushes `msg` to local stores.
  void ship_locally(const command_message& msg) {
    if (!store_manager().paths().empty())
      store_manager().push(msg);
    super::ship_locally(msg);
  }

  /// Sends `('peer', 'ok', <id>)` to peering listeners.
  void peer_connected(const peer_id_type& peer_id, const caf::actor& hdl) {
    auto i = pending_connections_.find(peer_id);
    if (i == pending_connections_.end())
      return;
    for (auto& promise : i->second.promises)
      promise.deliver(atom::peer::value, atom::ok::value, peer_id);
    pending_connections_.erase(i);
    super::peer_connected(peer_id, hdl);
  }

  // -- overridden member functions of caf::stream_manager ---------------------

  void handle(caf::inbound_path* path,
              caf::downstream_msg::batch& batch) override {
    BROKER_TRACE(BROKER_ARG(path) << BROKER_ARG(batch));
    auto& d = dref();
    using peer_batch = typename peer_trait::batch;
    if (batch.xs.template match_elements<peer_batch>()) {
      for (auto& x : batch.xs.template get_mutable_as<peer_batch>(0))
        d.handle_publication(x);
      return;
    }
    auto try_publish = [&](auto trait) {
      using batch_type = typename decltype(trait)::batch;
      if (batch.xs.template match_elements<batch_type>()) {
        for (auto& x : batch.xs.template get_mutable_as<batch_type>(0))
          d.publish(x);
        return true;
      }
      return false;
    };
    if (try_publish(worker_trait{}) || try_publish(store_trait{})
        || try_publish(var_trait{}))
      return;
    BROKER_ERROR("unexpected batch:" << deep_to_string(batch));
  }

  void handle(caf::inbound_path* path, caf::downstream_msg::close& x) override {
    BROKER_TRACE(BROKER_ARG(path) << BROKER_ARG(x));
    handle_impl(path, x);
  }

  void handle(caf::inbound_path* path,
              caf::downstream_msg::forced_close& x) override {
    BROKER_TRACE(BROKER_ARG(path) << BROKER_ARG(x));
    handle_impl(path, x);
  }

  void handle(caf::stream_slots slots, caf::upstream_msg::drop& x) override {
    BROKER_TRACE(BROKER_ARG(slots) << BROKER_ARG(x));
    handle_impl(slots, x);
  }

  void handle(caf::stream_slots slots,
              caf::upstream_msg::forced_drop& x) override {
    BROKER_TRACE(BROKER_ARG(slots) << BROKER_ARG(x));
    handle_impl(slots, x);
  }

  bool handle(caf::stream_slots slots,
              caf::upstream_msg::ack_open& x) override {
    CAF_LOG_TRACE(CAF_ARG(slots) << CAF_ARG(x));
    using caf::detail::make_scope_guard;
    auto rebind_from = caf::actor_cast<caf::actor>(x.rebind_from);
    auto rebind_to = caf::actor_cast<caf::actor>(x.rebind_to);
    bool abort_connection = false;
    if (rebind_from != rebind_to) {
      auto update_map = [&](auto& map) {
        auto i = map.find(rebind_from);
        if (i == map.end()) {
          BROKER_WARNING("received an ack_open from an unknown peer");
          return false;
        }
        auto slot = i->second;
        map.erase(i);
        if (!map.emplace(rebind_to, slot).second) {
          BROKER_ERROR("rebinding to an already existing entry!");
          return false;
        }
        return true;
      };
      auto update_tbl = [&] {
        auto predicate = [&](const auto& kvp) {
          return kvp.second.hdl == rebind_from;
        };
        auto e = tbl().end();
        auto i = std::find_if(tbl().begin(), e, predicate);
        if (i == e) {
          BROKER_WARNING("received an ack_open but no table entry exists");
          return false;
        }
        i->second.hdl = rebind_to;
        return true;
      };
      if (!update_map(hdl_to_istream_)) {
        abort_connection = true;
      } else if (!update_map(hdl_to_ostream_)) {
        hdl_to_istream_.erase(rebind_to);
        abort_connection = true;
      } else if (!update_tbl()) {
        hdl_to_istream_.erase(rebind_to);
        hdl_to_ostream_.erase(rebind_to);
        abort_connection = true;
      }
    }
    if (abort_connection || !caf::stream_manager::handle(slots, x)) {
      error reason = ec::peer_disconnect_during_handshake;
      disconnect(rebind_from, reason, x);
      return false;
    }
    return true;
  }

  bool done() const override {
    return !continuous() && pending_handshakes_ == 0 && inbound_paths_.empty()
           && out_.clean();
  }

  bool idle() const noexcept override {
    // Same as `stream_stage<...>`::idle().
    return out_.stalled() || (out_.clean() && this->inbound_paths_idle());
  }

  downstream_manager_type& out() override {
    return out_;
  }

  template <class... Fs>
  caf::behavior make_behavior(Fs... fs) {
    using detail::lift;
    auto& d = dref();
    return super::make_behavior(
      // Forward additional message handlers.
      std::move(fs)...,
      // Expose to member functions to messaging API.
      lift<atom::peer>(d, &Derived::start_peering),
      lift<atom::peer>(d, &Derived::handle_peering_request),
      lift<>(d, &Derived::handle_peering_handshake_1),
      lift<>(d, &Derived::handle_peering_handshake_2),
      lift<atom::join>(d, &Derived::add_worker),
      lift<atom::join>(d, &Derived::add_sending_worker),
      lift<atom::join, atom::store>(d, &Derived::add_sending_store),
      // Trigger peering to remotes.
      [this](atom::peer, const peer_id_type& remote_peer,
             const caf::actor& hdl) {
        dref().start_peering(remote_peer, hdl, self()->make_response_promise());
      },
      // Per-stream subscription updates.
      [this](atom::join, atom::update, caf::stream_slot slot,
             filter_type& filter) {
        dref().subscribe(filter);
        worker_manager().set_filter(slot, filter);
      },
      [this](atom::join, atom::update, caf::stream_slot slot,
             filter_type& filter, const caf::actor& listener) {
        dref().subscribe(filter);
        worker_manager().set_filter(slot, filter);
        self()->send(listener, true);
      },
      // Allow local publishers to hoook directly into the stream.
      [this](caf::stream<data_message> in) { add_unchecked_inbound_path(in); },
      [this](caf::stream<node_message_content> in) {
        add_unchecked_inbound_path(in);
      },
      // Special handlers for bypassing streams and/or forwarding.
      [this](atom::publish, atom::local, data_message msg) {
        publish_locally(msg);
      },
      [this](atom::unpeer, const caf::actor& hdl) { dref().unpeer(hdl); },
      [this](atom::unpeer, const peer_id_type& peer_id) {
        dref().unpeer(peer_id);
      });
  }

protected:
  /// Disconnects a peer by demand of the user.
  void unpeer(const peer_id_type& peer_id, const caf::actor& hdl) {
    BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG(hdl));
    // Keeps track of how many successful cleanup steps we perform. If this
    // counter is still 0 at the end of this function than this function was
    // called with an unknown peer.
    size_t cleanup_steps = 0;
    // Check whether we disconnect from the peer during the handshake.
    if (auto i = pending_connections_.find(peer_id);
        i != pending_connections_.end()) {
      ++cleanup_steps;
      error err = ec::peer_disconnect_during_handshake;
      for (auto& promise : i->second.promises)
        promise.deliver(err);
      pending_connections_.erase(i);
    }
    // Close the associated outbound path.
    if (auto i = hdl_to_ostream_.find(hdl); i != hdl_to_ostream_.end()) {
      ++cleanup_steps;
      out_.close(i->second);
      hdl_to_ostream_.erase(i);
    }
    // Close the associated inbound path.
    if (auto i = hdl_to_istream_.find(hdl); i != hdl_to_istream_.end()) {
      ++cleanup_steps;
      error reason;
      remove_input_path(i->second, reason, false);
      hdl_to_istream_.erase(i);
    }
    // The callback peer_removed ultimately removes the entry from the routing
    // table. Since some implementations of peer_removed may still query the
    // routing table, we only check whether peer_id exists in the routing table
    // rather than calling `erase`.
    cleanup_steps += tbl().count(peer_id);
    if (cleanup_steps > 0)
      dref().peer_removed(peer_id, hdl);
    else
      dref().cannot_remove_peer(peer_id);
  }

  /// Disconnects a peer by demand of the user.
  void unpeer(const peer_id_type& peer_id) {
    BROKER_TRACE(BROKER_ARG(peer_id));
    if (auto i = tbl().find(peer_id); i != tbl().end()) {
      auto hdl = i->second.hdl;
      dref().unpeer(peer_id, hdl);
      return;
    }
    auto i = pending_connections_.find(peer_id);
    if (i != pending_connections_.end()) {
      auto hdl = i->second.hdl;
      dref().unpeer(peer_id, hdl);
      return;
    }
    dref().cannot_remove_peer(peer_id);
  }

  /// Disconnects a peer by demand of the user.
  void unpeer(const caf::actor& hdl) {
    BROKER_TRACE(BROKER_ARG(hdl));
    if (auto peer_id = get_peer_id(tbl(), hdl)) {
      dref().unpeer(*peer_id, hdl);
      return;
    }
    auto predicate = [&](const auto& kvp) { return kvp.second.hdl == hdl; };
    if (auto i = std::find_if(pending_connections_.begin(),
                              pending_connections_.end(), predicate);
        i != pending_connections_.end()) {
      auto peer_id = i->first;
      dref().unpeer(peer_id, hdl);
      return;
    }
    dref().cannot_remove_peer(hdl);
  }

  /// Disconnects a peer as a result of receiving a `drop`, `forced_drop`,
  /// `close`, or `force_close` message.
  template <class Cause>
  void disconnect(const caf::actor& hdl, const error& reason, const Cause&) {
    BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(reason));
    static constexpr bool is_inbound
      = std::is_same<typename Cause::outer_type, caf::downstream_msg>::value;
    hdl_to_slot_map* channel;
    if constexpr (is_inbound)
      channel = &hdl_to_istream_;
    else
      channel = &hdl_to_ostream_;
    // Remove state associating the handle to a stream slot. If this fails,
    // check whether we lost the peer during the handshake.
    if (channel->erase(hdl) == 0) {
      auto predicate = [&](const auto& kvp) { return kvp.second.hdl == hdl; };
      auto e = pending_connections_.end();
      auto i = std::find_if(pending_connections_.begin(), e, predicate);
      if (i == e) {
        // Not an error. Usually means that we've received an
        // upstream_msg::drop message earlier that cleared all the state
        // already.
        BROKER_DEBUG("closed inbound path to unknown peer");
        return;
      }
      BROKER_DEBUG("lost peer during handshake:" << hdl);
      if (!i->second.promises.empty()) {
        BROKER_DEBUG("deliver" << i->second.promises.size() << "promises");
        error err = ec::peer_disconnect_during_handshake;
        for (auto& promise : i->second.promises)
          promise.deliver(err);
      }
      pending_connections_.erase(i);
      return;
    }
    // Fetch the node ID from the routing table and invoke callbacks.
    auto& d = dref();
    if (auto peer_id = get_peer_id(tbl(), hdl)) {
      // peer::peer_disconnected ultimately removes the entry from the table.
      d.peer_disconnected(*peer_id, hdl, reason);
    } else {
      BROKER_ERROR("closed inbound path to a peer without routing table entry");
    }
    if constexpr (is_inbound) {
      // Close the associated outbound path.
      auto i = hdl_to_ostream_.find(hdl);
      if (i == hdl_to_ostream_.end()) {
        BROKER_WARNING(
          "closed inbound path to a peer that had no outbound path");
        return;
      }
      out_.close(i->second);
      hdl_to_ostream_.erase(i);
    } else {
      // Close the associated inbound path.
      auto i = hdl_to_istream_.find(hdl);
      if (i == hdl_to_istream_.end()) {
        BROKER_WARNING(
          "closed inbound path to a peer that had no outbound path");
        return;
      }
      remove_input_path(i->second, reason, false);
      hdl_to_istream_.erase(i);
    }
  }

  template <class Cause>
  void handle_impl(caf::inbound_path* path, Cause& cause) {
    if (path->hdl == nullptr) {
      BROKER_ERROR("closed inbound path with invalid communication handle");
    } else {
      auto hdl = caf::actor_cast<caf::actor>(path->hdl);
      if constexpr (std::is_same<caf::downstream_msg::close, Cause>::value) {
        error dummy;
        disconnect(hdl, dummy, cause);
      } else {
        disconnect(hdl, cause.reason, cause);
      }
    }
    caf::stream_manager::handle(path, cause);
  }

  template <class Cause>
  void handle_impl(caf::stream_slots slots, Cause& cause) {
    auto path = out_.path(slots.receiver);
    if (!path || path->hdl == nullptr) {
      BROKER_DEBUG("closed outbound path with invalid communication handle");
    } else {
      auto hdl = caf::actor_cast<caf::actor>(path->hdl);
      if constexpr (std::is_same<caf::upstream_msg::drop, Cause>::value) {
        error dummy;
        disconnect(hdl, dummy, cause);
      } else {
        disconnect(hdl, cause.reason, cause);
      }
    }
    caf::stream_manager::handle(slots, cause);
  }

  /// Organizes downstream communication to peers as well as local subscribers.
  downstream_manager_type out_;

  /// Maps communication handles to output slots.
  hdl_to_slot_map hdl_to_ostream_;

  /// Maps communication handles to input slots.
  hdl_to_slot_map hdl_to_istream_;

  /// Stores nodes we have in-flight peering handshakes to.
  std::map<peer_id_type, pending_connection> pending_connections_;

private:
  Derived& dref() {
    return static_cast<Derived&>(*this);
  }
};

} // namespace broker::alm
