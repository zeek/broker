#pragma once

#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <caf/actor.hpp>
#include <caf/actor_addr.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/broadcast_downstream_manager.hpp>
#include <caf/cow_tuple.hpp>
#include <caf/detail/scope_guard.hpp>
#include <caf/fused_downstream_manager.hpp>
#include <caf/fwd.hpp>
#include <caf/message.hpp>
#include <caf/sec.hpp>
#include <caf/settings.hpp>
#include <caf/stream_manager.hpp>
#include <caf/stream_slot.hpp>

#include "broker/atoms.hh"
#include "broker/data.hh"
#include "broker/defaults.hh"
#include "broker/detail/assert.hh"
#include "broker/detail/central_dispatcher.hh"
#include "broker/detail/filesystem.hh"
#include "broker/detail/generator_file_writer.hh"
#include "broker/detail/prefix_matcher.hh"
#include "broker/detail/unipath_manager.hh"
#include "broker/error.hh"
#include "broker/filter_type.hh"
#include "broker/internal_command.hh"
#include "broker/logger.hh"
#include "broker/message.hh"
#include "broker/peer_filter.hh"
#include "broker/status.hh"
#include "broker/topic.hh"

namespace broker::alm {

/// Sets up a configurable stream manager to act as a distribution tree for
/// Broker.
template <class Derived, class PeerId>
class stream_transport : public detail::unipath_manager::observer {
public:
  // -- constants --------------------------------------------------------------

  /// Configures how many (additional) items the stream transport caches for
  /// published events that bypass streaming.
  static constexpr size_t item_stash_replenish_size = 32;

  // -- member types -----------------------------------------------------------

  using peer_id_type = PeerId;

  using communication_handle_type = caf::actor;

  using manager_ptr = detail::unipath_manager_ptr;

  struct pending_connection {
    manager_ptr mgr;
    caf::response_promise rp;
  };

  /// Maps peer actor handles to their stream managers.
  using hdl_to_mgr_map = std::unordered_map<caf::actor, manager_ptr>;

  /// Maps stream managers to peer actor handles.
  using mgr_to_hdl_map = std::unordered_map<manager_ptr, caf::actor>;

  // -- constructors, destructors, and assignment operators --------------------

  stream_transport(caf::event_based_actor* self, const filter_type& filter)
    : self_(self), dispatcher_(self) {
    using caf::get_or;
    auto& cfg = self->system().config();
    auto meta_dir = get_or(cfg, "broker.recording-directory",
                           defaults::recording_directory);
    if (!meta_dir.empty() && detail::is_directory(meta_dir)) {
      auto file_name = meta_dir + "/messages.dat";
      recorder_ = detail::make_generator_file_writer(file_name);
      if (recorder_ == nullptr) {
        BROKER_WARNING("cannot open recording file" << file_name);
      } else {
        BROKER_DEBUG("opened file for recording:" << file_name);
        remaining_records_ = get_or(cfg, "broker.output-generator-file-cap",
                                    defaults::output_generator_file_cap);
      }
    }
  }

  // -- initialization ---------------------------------------------------------

  template <class... Fs>
  caf::behavior make_behavior(Fs... fs) {
    return {std::move(fs)...};
  }

  // -- properties -------------------------------------------------------------

  caf::event_based_actor* self() noexcept {
    return self_;
  }

  auto& pending_connections() noexcept {
    return pending_connections_;
  }

  uint16_t ttl() const noexcept {
    return dref().options().ttl;
  }

  // -- peer management --------------------------------------------------------

  /// Queries whether `hdl` is a known peer.
  [[nodiscard]] bool connected_to(const caf::actor& hdl) const {
    return hdl_to_mgr_.count(hdl) != 0;
  }

  /// Drops a peer either due to an error or due to a graceful disconnect.
  void drop_peer(const caf::actor& hdl, bool graceful, const error& reason) {
    if (auto i = hdl_to_mgr_.find(hdl); i != hdl_to_mgr_.end()) {
      auto mgr = i->second;
      mgr_to_hdl_.erase(mgr);
      hdl_to_mgr_.erase(i);
      if (graceful)
        BROKER_DEBUG(hdl.node() << "disconnected gracefully");
      else
        BROKER_DEBUG(hdl.node() << "disconnected abnormally:" << reason);
      dref().peer_disconnected(hdl.node(), hdl, reason);
    } else if (auto j = pending_connections_.find(hdl);
               j != pending_connections_.end()) {
      BROKER_DEBUG("peer failed to connect");
      auto err = make_error(ec::peer_disconnect_during_handshake);
      j->second.rp.deliver(err);
      pending_connections_.erase(j);
      dref().peer_unavailable(hdl.node(), hdl, err);
    }
    // Shut down when the last peer stops listening.
    if (dref().shutting_down() && pending_connections_.empty()
        && hdl_to_mgr_.empty())
      self()->quit(caf::exit_reason::user_shutdown);
  }

  /// Disconnects a peer by demand of the user.
  void unpeer(const peer_id_type& peer_id, const caf::actor& hdl) {
    BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG(hdl));
    if (auto i = hdl_to_mgr_.find(hdl); i != hdl_to_mgr_.end()) {
      auto mgr = i->second;
      mgr->unobserve();
      mgr->stop();
      mgr_to_hdl_.erase(mgr);
      hdl_to_mgr_.erase(i);
      dref().peer_removed(peer_id, hdl);
    } else if (auto j = pending_connections_.find(hdl);
               j != pending_connections_.end()) {
      auto mgr = j->second.mgr;
      mgr->unobserve();
      mgr->stop();
      auto err = make_error(ec::peer_disconnect_during_handshake);
      j->second.rp.deliver(err);
      pending_connections_.erase(j);
      dref().peer_unavailable(peer_id, hdl, err);
    } else {
      dref().cannot_remove_peer(peer_id, hdl);
    }
    // Shut down when the last peer stops listening.
    if (dref().shutting_down() && pending_connections_.empty()
        && mgr_to_hdl_.empty())
      self()->quit(caf::exit_reason::user_shutdown);
  }

  /// Disconnects a peer by demand of the user.
  void unpeer(const caf::actor& hdl) {
    BROKER_TRACE(BROKER_ARG(hdl));
    if (hdl)
      unpeer(hdl.node(), hdl);
  }

  /// Starts the handshake process for a new peering (step #1 in core_actor.cc).
  /// @returns `false` if the peer is already connected, `true` otherwise.
  /// @param peer_hdl Handle to the peering (remote) core actor.
  /// @param peer_filter Filter of our peer.
  /// @param send_own_filter Sends a `(filter, self)` handshake if `true`,
  ///                        `('ok', self)` otherwise.
  /// @pre `current_sender() != nullptr`
  template <bool SendOwnFilter>
  auto start_handshake(const caf::actor& peer_hdl, filter_type peer_filter) {
    BROKER_TRACE(BROKER_ARG(peer_hdl) << BROKER_ARG(peer_filter));
    using result_type = std::conditional_t<
      SendOwnFilter,
      caf::outbound_stream_slot<node_message, filter_type, caf::actor>,
      caf::outbound_stream_slot<node_message, atom::ok, caf::actor>>;
    // Check whether we already send outbound traffic to the peer. Could use
    // `CAF_ASSERT` instead, because this must'nt get called for known peers.
    if (hdl_to_mgr_.count(peer_hdl) != 0) {
      BROKER_ERROR("peer already connected");
      return result_type{};
    }
    // Add outbound path to the peer.
    auto self_hdl = caf::actor_cast<caf::actor>(self());
    if constexpr (SendOwnFilter) {
      if (auto i = pending_connections_.count(peer_hdl) == 0) {
        auto mgr = detail::make_peer_manager(&dispatcher_, this);
        mgr->filter(std::move(peer_filter));
        pending_connections_[peer_hdl].mgr = mgr;
        return mgr->template add_unchecked_outbound_path<node_message>(
          peer_hdl, std::make_tuple(dref().filter(), std::move(self_hdl)));
      } else {
        BROKER_ERROR("already connecting to peer");
        return result_type{};
      }
    } else if (auto i = pending_connections_.find(peer_hdl);
               i != pending_connections_.end()) {
      auto mgr = i->second.mgr;
      mgr->filter(std::move(peer_filter));
      return mgr->template add_unchecked_outbound_path<node_message>(
        peer_hdl, std::make_tuple(atom::ok_v, std::move(self_hdl)));
    } else {
      BROKER_ERROR("received handshake #2 from unknown peer");
      return result_type{};
    }
  }

  /// Initiates peering between this peer and `remote_peer`.
  void start_peering(const peer_id_type&, caf::actor remote_peer,
                     caf::response_promise rp) {
    BROKER_TRACE(BROKER_ARG(remote_peer));
    // Sanity checking.
    if (remote_peer == nullptr) {
      rp.deliver(caf::sec::invalid_argument);
      return;
    }
    // Ignore repeated peering requests without error.
    if (pending_connections().count(remote_peer) > 0
        || connected_to(remote_peer)) {
      rp.deliver(caf::unit);
      return;
    }
    // Create necessary state and send message to remote core.
    auto mgr = make_peer_manager(&dispatcher_, this);
    pending_connections().emplace(remote_peer,
                                  pending_connection{mgr, std::move(rp)});
    self()->send(self() * remote_peer, atom::peer_v, dref().filter(), self());
    self()->monitor(remote_peer);
  }

  /// Acknowledges an incoming peering request (step #2/3 in core_actor.cc).
  /// @param peer_hdl Handle to the peering (remote) core actor.
  /// @returns `false` if the peer is already connected, `true` otherwise.
  /// @pre Current message is an `open_stream_msg`.
  bool ack_peering(const caf::stream<node_message>& in,
                   const caf::actor& peer_hdl) {
    BROKER_TRACE(BROKER_ARG(peer_hdl));
    BROKER_ASSERT(hdl_to_mgr_.count(peer_hdl) == 0);
    if (auto i = pending_connections_.find(peer_hdl);
        i != pending_connections_.end()) {
      if (!i->second.mgr->has_inbound_path()) {
        i->second.mgr->add_unchecked_inbound_path(in);
        return true;
      } else {
        BROKER_ERROR("ack_peering called, but an inbound path already exists");
        return false;
      }
    } else {
      BROKER_ERROR("ack_peering but no peering started yet");
      return false;
    }
  }

  /// Updates the filter of an existing peer.
  bool update_peer(const caf::actor& hdl, filter_type filter) {
    BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(filter));
    if (auto i = hdl_to_mgr_.find(hdl); i != hdl_to_mgr_.end()) {
      i->second->filter(std::move(filter));
      return true;
    } else {
      BROKER_DEBUG("cannot update filter of unknown peer");
      return false;
    }
  }

  void try_finalize_handshake(const caf::actor& hdl) {
    if (auto i = pending_connections_.find(hdl);
        i != pending_connections_.end()) {
      if (auto mgr = i->second.mgr; mgr->fully_connected()) {
        mgr->unblock_inputs();
        dispatcher_.add(mgr);
        hdl_to_mgr_.emplace(hdl, mgr);
        mgr_to_hdl_.emplace(mgr, hdl);
        i->second.rp.deliver(hdl);
        pending_connections_.erase(i);
        dref().peer_connected(hdl.node(), hdl);
      }
    }
  }

  // -- filter management ------------------------------------------------------

  void set_filter(caf::stream_slot out_slot, filter_type filter) {
    auto i = std::find_if(dispatcher_.managers().begin(),
                          dispatcher_.managers().end(),
                          [out_slot](const auto& ptr) {
                            return ptr->outbound_path_slot() == out_slot;
                          });
    if (i != dispatcher_.managers().end())
      (*i)->filter(std::move(filter));
  }

  // -- management of worker and storage streams -------------------------------

  /// Adds the sender of the current message as worker by starting an output
  /// stream to it.
  /// @pre `current_sender() != nullptr`
  auto add_worker(filter_type filter) {
    BROKER_TRACE(BROKER_ARG(filter));
    dref().subscribe(filter);
    auto mgr = make_data_sink(&dispatcher_, std::move(filter));
    auto res = mgr->template add_unchecked_outbound_path<data_message>();
    BROKER_ASSERT(res != caf::invalid_stream_slot);
    return res;
  }

  /// Subscribes `self->sender()` to `store_manager()`.
  auto add_sending_store(filter_type filter) {
    BROKER_TRACE(BROKER_ARG(filter));
    dref().subscribe(filter);
    auto mgr = make_command_sink(&dispatcher_, std::move(filter));
    auto res = mgr->template add_unchecked_outbound_path<command_message>();
    BROKER_ASSERT(res != caf::invalid_stream_slot);
    return res;
  }

  /// Subscribes `hdl` to `store_manager()`.
  caf::error add_store(const caf::actor& hdl, const filter_type& filter) {
    BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(filter));
    auto mgr = make_command_sink(&dispatcher_, filter);
    auto res = mgr->template add_unchecked_outbound_path<command_message>(hdl);
    if (res != caf::invalid_stream_slot) {
      dref().subscribe(filter);
      return caf::none;
    } else {
      return caf::sec::cannot_add_downstream;
    }
  }

  // -- selectively pushing data into the streams ------------------------------

  /// Pushes messages to local subscribers without forwarding it to peers.
  template <class T>
  void local_push(T msg) {
    BROKER_TRACE(BROKER_ARG(msg));
    auto wrapped = make_node_message(std::move(msg), ttl());
    dispatcher_.enqueue(nullptr, detail::item_scope::local,
                        caf::make_span(&wrapped, 1));
  }

  /// Pushes messages to peers without forwarding it to local subscribers.
  void remote_push(node_message msg) {
    BROKER_TRACE(BROKER_ARG(msg));
    dispatcher_.enqueue(nullptr, detail::item_scope::remote,
                        caf::make_span(&msg, 1));
  }

  /// Pushes data to peers.
  void push(data_message msg) {
    remote_push(make_node_message(std::move(msg), ttl()));
  }

  /// Pushes data to peers.
  void push(command_message msg) {
    remote_push(make_node_message(std::move(msg), ttl()));
  }

  /// Pushes data to peers.
  void push(node_message msg) {
    remote_push(std::move(msg));
  }

  // -- communication that bypasses the streams --------------------------------

  void ship(data_message& msg, const communication_handle_type& hdl) {
    self()->send(hdl, atom::publish_v, atom::local_v, std::move(msg));
  }

  template <class T>
  void ship(T& msg) {
    push(std::move(msg));
  }

  void ship(node_message& msg) {
    push(std::move(msg));
  }

  template <class T>
  void publish(T msg) {
    dref().ship(msg);
  }

  void publish(node_message_content msg) {
    visit([this](auto& x) { dref().ship(x); }, msg);
  }

  // -- unipath manager callbacks ----------------------------------------------

  void closing(detail::unipath_manager* ptr, bool graceful,
               const error& reason) override {
    drop_peer(ptr->hdl(), graceful, reason);
  }

  void downstream_connected(detail::unipath_manager*,
                            const caf::actor& hdl) override {
    try_finalize_handshake(hdl);
  }

  // -- overridden member functions of caf::stream_manager ---------------------

  /// Applies `f` to each peer.
  template <class F>
  void for_each_peer(F f) {
    auto peers = peer_handles();
    std::for_each(peers.begin(), peers.end(), std::move(f));
  }

  /// Returns all known peers.
  auto peer_handles() {
    std::vector<caf::actor> peers;
    for (auto& kvp : hdl_to_mgr_)
      peers.emplace_back(kvp.first);
    return peers;
  }

  /// Finds the first peer handle that satisfies the predicate.
  template <class Predicate>
  caf::actor find_output_peer_hdl(Predicate pred) {
    for (auto& kvp : hdl_to_mgr_)
      if (pred(kvp.first))
        return kvp.first;
    return nullptr;
  }

  /// Applies `f` to each filter.
  template <class F>
  void for_each_filter(F f) {
    for (auto& kvp : mgr_to_hdl_)
      if (kvp.first->message_type() == caf::type_id_v<node_message>)
        f(kvp.first->filter());
  }

  /// Checks whether the predicate `f` holds for any @ref unicast_manager object
  /// that represents a remote peer.
  template <class Predicate>
  bool any_peer_manager(Predicate f) {
    for (auto& kvp : mgr_to_hdl_)
      if (f(kvp.first))
        return true;
    return false;
  }

  // -- fallback implementations to enable forwarding chains -------------------

  void subscribe(const filter_type&) {
    // nop
  }

  // -- callbacks --------------------------------------------------------------

  /// Called whenever new data for local subscribers became available.
  /// @param msg Data or command message, either received by peers or generated
  ///            from a local publisher.
  /// @tparam T Either ::data_message or ::command_message.
  template <class T>
  void ship_locally(T msg) {
    local_push(std::move(msg));
  }

  /// Called whenever this peer established a new connection.
  /// @param peer_id ID of the newly connected peer.
  /// @param hdl Communication handle for exchanging messages with the new peer.
  ///            The handle is default-constructed if no direct connection
  ///            exists (yet).
  /// @note The new peer gets stored in the routing table *before* calling this
  ///       member function.
  void peer_connected([[maybe_unused]] const peer_id_type& peer_id,
                      [[maybe_unused]] const communication_handle_type& hdl) {
    // nop
  }

  /// Called whenever this peer lost a connection to a remote peer.
  /// @param peer_id ID of the disconnected peer.
  /// @param hdl Communication handle of the disconnected peer.
  /// @param reason None if we closed the connection gracefully, otherwise
  ///               contains the transport-specific error code.
  void peer_disconnected([[maybe_unused]] const peer_id_type& peer_id,
                         [[maybe_unused]] const communication_handle_type& hdl,
                         [[maybe_unused]] const error& reason) {
    // nop
  }

  /// Called whenever this peer removed a direct connection to a remote peer.
  /// @param peer_id ID of the removed peer.
  /// @param hdl Communication handle of the removed peer.
  void peer_removed([[maybe_unused]] const peer_id_type& peer_id,
                    [[maybe_unused]] const communication_handle_type& hdl) {
    // nop
  }

  /// Called whenever the user tried to unpeer from an unconnected peer.
  /// @param addr Host information for the unconnected peer.
  void cannot_remove_peer([[maybe_unused]] const network_info& addr) {
    // nop
  }

  /// Called whenever the user tried to unpeer from an unconnected peer.
  /// @param peer_id ID of the unconnected peer.
  /// @param hdl Communication handle of the unconnected peer (may be null).
  void
  cannot_remove_peer([[maybe_unused]] const peer_id_type& peer_id,
                     [[maybe_unused]] const communication_handle_type& hdl) {
    // nop
  }

  /// Called whenever establishing a connection to a remote peer failed.
  /// @param addr Host information for the unavailable peer.
  void peer_unavailable([[maybe_unused]] const network_info& addr) {
    // nop
  }

  /// Called whenever we could obtain a connection handle to a remote peer but
  /// received a `down_msg` before completing the handshake.
  /// @param peer_id ID of the unavailable peer.
  /// @param hdl Communication handle of the unavailable peer.
  /// @param reason Exit reason of the unavailable peer.
  void peer_unavailable([[maybe_unused]] const peer_id_type& peer_id,
                        [[maybe_unused]] const communication_handle_type& hdl,
                        [[maybe_unused]] const error& reason) {
    // nop
  }

protected:
  manager_ptr mgr_by_hdl(const caf::actor& hdl) {
    if (auto i = hdl_to_mgr_.find(hdl); i != hdl_to_mgr_.end())
      return i->second;
    else if (auto j = pending_connections_.find(hdl);
             j != pending_connections_.end())
      return j->second.mgr;
    else
      return nullptr;
  }

  caf::actor hdl_by_mgr(const manager_ptr& mgr) {
    if (auto i = mgr_to_hdl_.find(mgr); i != mgr_to_hdl_.end())
      return i->second;
    else if (auto j = pending_connections_.find(mgr);
             j != pending_connections_.end())
      return j->second.mgr;
    else
      return nullptr;
  }

  /// Pointer to the actor that owns this state.
  caf::event_based_actor* self_;

  /// Our dispatcher "singleton" that holds the item allocator as well as
  /// pointers to all active unipath managers for outbound traffic.
  detail::central_dispatcher dispatcher_;

  /// Maps peer handles to their respective unipath manager.
  hdl_to_mgr_map hdl_to_mgr_;

  /// Maps unipath managers to their respective peer handle.
  mgr_to_hdl_map mgr_to_hdl_;

  /// Maps pending peer handles to output IDs. An invalid stream ID indicates
  /// that only "step #0" was performed so far. An invalid stream ID corresponds
  /// to `peer_status::connecting` and a valid stream ID cooresponds to
  /// `peer_status::connected`. The status for a given handle `x` is
  /// `peer_status::peered` if `governor->has_peer(x)` returns true.
  std::unordered_map<caf::actor, pending_connection> pending_connections_;

  /// Helper for recording meta data of published messages.
  detail::generator_file_writer_ptr recorder_;

  /// Counts down when using a `recorder_` to cap maximum file entries.
  size_t remaining_records_ = 0;

private:
  Derived& dref() {
    return static_cast<Derived&>(*this);
  }

  const Derived& dref() const {
    return static_cast<const Derived&>(*this);
  }
};

} // namespace broker::alm
