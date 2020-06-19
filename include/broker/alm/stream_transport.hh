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
#include <caf/settings.hpp>
#include <caf/stream_manager.hpp>
#include <caf/stream_slot.hpp>

#include "broker/atoms.hh"
#include "broker/data.hh"
#include "broker/defaults.hh"
#include "broker/detail/assert.hh"
#include "broker/detail/filesystem.hh"
#include "broker/detail/generator_file_writer.hh"
#include "broker/detail/prefix_matcher.hh"
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
class stream_transport : public caf::stream_manager {
public:
  // -- member types -----------------------------------------------------------

  using peer_id_type = PeerId;

  using message_type = generic_node_message<PeerId>;

  struct pending_connection {
    caf::stream_slot slot;
    caf::response_promise rp;
  };

  /// Type to store a TTL for messages forwarded to peers.
  using ttl = uint16_t;

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
    using batch = std::vector<typename message_type::value_type>;
  };

  /// Streaming-related types for peers.
  struct peer_trait {
    /// Type of a single element in the stream.
    using element = message_type;

    using batch = std::vector<element>;

    /// Type of the downstream_manager that broadcasts data to local actors.
    using manager = caf::broadcast_downstream_manager<element, peer_filter,
                                                      peer_filter_matcher>;
  };

  /// Composed downstream_manager type for bundled dispatching.
  using downstream_manager_type
    = caf::fused_downstream_manager<typename peer_trait::manager,
                                    typename worker_trait::manager,
                                    typename store_trait::manager>;

  /// Maps actor handles to path IDs.
  using hdl_to_slot_map = std::unordered_map<caf::actor, caf::stream_slot>;

  /// Maps path IDs to actor handles.
  using slot_to_hdl_map = std::unordered_map<caf::stream_slot, caf::actor>;

  /// Stream handshake in step 1 that includes our own filter. The receiver
  /// replies with a step2 handshake.
  using step1_handshake = caf::outbound_stream_slot<message_type,
                                                    filter_type,
                                                    caf::actor>;

  /// Stream handshake in step 2. The receiver already has our filter
  /// installed.
  using step2_handshake = caf::outbound_stream_slot<message_type,
                                                    caf::atom_value,
                                                    caf::actor>;

  // -- constructors, destructors, and assignment operators --------------------

  stream_transport(caf::event_based_actor* self, const filter_type& filter)
    : caf::stream_manager(self), out_(this), remaining_records_(0) {
    continuous(true);
    // TODO: use filter
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

  // -- properties -------------------------------------------------------------

  caf::event_based_actor* self() noexcept {
    // Our only constructor accepts an event-based actor. Hence, we know for
    // sure that this case is safe, even though the base type stores self_ as a
    // scheduled_actor pointer.
    return static_cast<caf::event_based_actor*>(this->self_);
  }

  auto& pending_connections() noexcept {
    return pending_connections_;
  }

  /// Returns the downstream_manager for peer traffic.
  auto& peer_manager() noexcept {
    return out().template get<typename peer_trait::manager>();
  }

  /// Returns the downstream_manager for worker traffic.
  auto& worker_manager() noexcept {
    return out().template get<typename worker_trait::manager>();
  }

  /// Returns the downstream_manager for data store traffic.
  auto& store_manager() noexcept {
    return out().template get<typename store_trait::manager>();
  }

  // -- streaming helper functions ---------------------------------------------

  void ack_open_success(caf::stream_slot slot,
                        const caf::actor_addr& rebind_from,
                        caf::strong_actor_ptr rebind_to) {
    BROKER_TRACE(BROKER_ARG(slot)
                 << BROKER_ARG(rebind_from) << BROKER_ARG(rebind_to));
    if (rebind_from != rebind_to) {
      BROKER_DEBUG("rebind occurred" << BROKER_ARG(slot)
                                     << BROKER_ARG(rebind_from)
                                     << BROKER_ARG(rebind_to));
      peer_manager().filter(slot).first
        = caf::actor_cast<caf::actor_addr>(rebind_to);
    }
  }

  void ack_open_failure(caf::stream_slot slot,
                        const caf::actor_addr& rebind_from,
                        caf::strong_actor_ptr rebind_to) {
    BROKER_TRACE(BROKER_ARG(slot)
                 << BROKER_ARG(rebind_from) << BROKER_ARG(rebind_to));
    CAF_IGNORE_UNUSED(rebind_from);
    CAF_IGNORE_UNUSED(rebind_to);
    auto i = ostream_to_peer_.find(slot);
    if (i != ostream_to_peer_.end()) {
      auto hdl = i->second;
      remove_peer(hdl, make_error(caf::sec::invalid_stream_state), false,
                  false);
    }
  }

  void push_to_substreams(std::vector<caf::message> xs) {
    // Dispatch on the content of `xs`.
    for (auto& x : xs) {
      if (x.match_elements<topic, data>()) {
        x.force_unshare();
        worker_manager().push(std::move(x.get_mutable_as<topic>(0)),
                              std::move(x.get_mutable_as<data>(1)));
      } else if (x.match_elements<topic, internal_command>()) {
        x.force_unshare();
        store_manager().push(std::move(x.get_mutable_as<topic>(0)),
                             std::move(x.get_mutable_as<internal_command>(1)));
      }
    }
    worker_manager().emit_batches();
    store_manager().emit_batches();
  }

  // -- status updates to the state --------------------------------------------

  void peer_lost(const caf::actor& hdl) {
    BROKER_TRACE(BROKER_ARG(hdl));
    dref().template emit_status<sc::peer_lost>(hdl, "lost remote peer");
    if (dref().shutting_down)
      return;
    auto x = dref().cache.find(hdl);
    if (!x || x->retry == timeout::seconds(0))
      return;
    BROKER_INFO("will try reconnecting to" << *x << "in"
                                           << to_string(x->retry));
    self()->delayed_send(self(), x->retry, atom::peer::value,
                         atom::retry::value, *x);
  }

  void peer_removed(const caf::actor& hdl) {
    BROKER_TRACE(BROKER_ARG(hdl));
    dref().template emit_status<sc::peer_removed>(hdl, "removed peering");
  }

  // -- peer management --------------------------------------------------------

  /// Queries whether `hdl` is a known peer.
  bool connected_to(const caf::actor& hdl) const {
    return hdl_to_ostream_.count(hdl) != 0 || hdl_to_istream_.count(hdl) != 0;
  }

  /// Block peer messages from being handled.  They are buffered until unblocked.
  void block_peer(caf::actor peer) {
    blocked_peers.emplace(std::move(peer));
  }

  /// Unblock peer messages and flush any buffered messages immediately.
  void unblock_peer(caf::actor peer) {
    blocked_peers.erase(peer);
    auto it = blocked_msgs.find(peer);
    if (it == blocked_msgs.end())
      return;
    auto pit = hdl_to_istream_.find(peer);
    if (pit == hdl_to_istream_.end()) {
      blocked_msgs.erase(it);
      BROKER_DEBUG(
        "dropped batches after unblocking peer: path no longer exists" << peer);
      return;
    }
    auto& slot = pit->second;
    auto sap = caf::actor_cast<caf::strong_actor_ptr>(peer);
    for (auto& batch : it->second) {
      BROKER_DEBUG("handle blocked batch" << peer);
      handle_batch(sap, batch);
    }
    blocked_msgs.erase(it);
  }

  /// Starts the handshake process for a new peering (step #1 in core_actor.cc).
  /// @returns `false` if the peer is already connected, `true` otherwise.
  /// @param peer_hdl Handle to the peering (remote) core actor.
  /// @param peer_filter Filter of our peer.
  /// @param send_own_filter Sends a `(filter, self)` handshake if `true`,
  ///                        `('ok', self)` otherwise.
  /// @pre `current_sender() != nullptr`
  template <bool SendOwnFilter>
  typename std::conditional<
    SendOwnFilter,
    step1_handshake,
    step2_handshake
  >::type
  start_peering(const caf::actor& peer_hdl, filter_type peer_filter) {
    BROKER_TRACE(BROKER_ARG(peer_hdl) << BROKER_ARG(peer_filter));
    // Token for static dispatch of add().
    std::integral_constant<bool, SendOwnFilter> send_own_filter_token;
    // Check whether we already send outbound traffic to the peer. Could use
    // `CAF_ASSERT` instead, because this must'nt get called for known peers.
    if (hdl_to_ostream_.count(peer_hdl) != 0) {
      BROKER_ERROR("peer already connected");
      return {};
    }
    // Add outbound path to the peer.
    auto slot = add(send_own_filter_token, peer_hdl);
    // Make sure the peer receives the correct traffic.
    out().template assign<typename peer_trait::manager>(slot);
    peer_manager().set_filter(
      slot, std::make_pair(peer_hdl.address(), std::move(peer_filter)));
    // Add bookkeeping state for our new peer.
    add_opath(slot, peer_hdl);
    return slot;
  }

  /// Acknowledges an incoming peering request (step #2/3 in core_actor.cc).
  /// @param peer_hdl Handle to the peering (remote) core actor.
  /// @returns `false` if the peer is already connected, `true` otherwise.
  /// @pre Current message is an `open_stream_msg`.
  void ack_peering(const caf::stream<message_type>& in,
                   const caf::actor& peer_hdl) {
    BROKER_TRACE(BROKER_ARG(peer_hdl));
    // Check whether we already receive inbound traffic from the peer. Could use
    // `CAF_ASSERT` instead, because this must'nt get called for known peers.
    if (hdl_to_istream_.count(peer_hdl) != 0) {
      BROKER_ERROR("peer already connected");
      return;
    }
    // Add inbound path for our peer.
    auto slot = this->add_unchecked_inbound_path(in);
    add_ipath(slot, peer_hdl);
  }

  /// Queries whether we have an outbound path to `hdl`.
  bool has_outbound_path_to(const caf::actor& peer_hdl) {
    return hdl_to_ostream_.count(peer_hdl) != 0;
  }

  /// Queries whether we have an inbound path from `hdl`.
  bool has_inbound_path_from(const caf::actor& peer_hdl) {
    return hdl_to_istream_.count(peer_hdl) != 0;
  }

  /// Removes a peer, aborting any stream to and from that peer.
  bool remove_peer(const caf::actor& hdl, caf::error reason, bool silent,
                   bool graceful_removal) {
    BROKER_TRACE(BROKER_ARG(hdl));
    int performed_erases = 0;
    { // lifetime scope of first iterator pair
      auto e = hdl_to_ostream_.end();
      auto i = hdl_to_ostream_.find(hdl);
      if (i != e) {
        BROKER_DEBUG("remove outbound path to peer:" << hdl);
        ++performed_erases;
        out().remove_path(i->second, reason, silent);
        ostream_to_peer_.erase(i->second);
        hdl_to_ostream_.erase(i);
      }
    }
    { // lifetime scope of second iterator pair
      auto e = hdl_to_istream_.end();
      auto i = hdl_to_istream_.find(hdl);
      if (i != e) {
        BROKER_DEBUG("remove inbound path to peer:" << hdl);
        ++performed_erases;
        this->remove_input_path(i->second, reason, silent);
        istream_to_hdl_.erase(i->second);
        hdl_to_istream_.erase(i);
      }
    }
    if (performed_erases == 0) {
      BROKER_DEBUG("no path was removed for peer:" << hdl);
      return false;
    }
    if (graceful_removal)
      peer_removed(hdl);
    else
      peer_lost(hdl);
    dref().cache.remove(hdl);
    if (dref().shutting_down && hdl_to_ostream_.empty()) {
      // Shutdown when the last peer stops listening.
      self()->quit(caf::exit_reason::user_shutdown);
    } else {
      // See whether we can make progress without that peer in the mix.
      this->push();
    }
    return true;
  }

  /// Updates the filter of an existing peer.
  bool update_peer(const caf::actor& hdl, filter_type filter) {
    BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(filter));
    auto e = hdl_to_ostream_.end();
    auto i = hdl_to_ostream_.find(hdl);
    if (i == e) {
      BROKER_DEBUG("cannot update filter on unknown peer");
      return false;
    }
    peer_manager().filter(i->second).second = std::move(filter);
    return true;
  }

  // -- management of worker and storage streams -------------------------------

  /// Adds the sender of the current message as worker by starting an output
  /// stream to it.
  /// @pre `current_sender() != nullptr`
  caf::outbound_stream_slot<typename worker_trait::element>
  add_worker(filter_type filter) {
    BROKER_TRACE(BROKER_ARG(filter));
    auto slot = this->template add_unchecked_outbound_path<
      typename worker_trait::element>();
    if (slot != caf::invalid_stream_slot) {
      out().template assign<typename worker_trait::manager>(slot);
      worker_manager().set_filter(slot, std::move(filter));
    }
    return slot;
  }

  /// Adds the sender of the current message as store by starting an output
  /// stream to it.
  /// @pre `current_sender() != nullptr`
  caf::outbound_stream_slot<typename store_trait::element>
  add_store(filter_type filter) {
    CAF_LOG_TRACE(CAF_ARG(filter));
    auto slot = this->template add_unchecked_outbound_path<
      typename store_trait::element>();
    if (slot != caf::invalid_stream_slot) {
      out().template assign<typename store_trait::manager>(slot);
      store_manager().set_filter(slot, std::move(filter));
    }
    return slot;
  }

  // -- selectively pushing data into the streams ------------------------------

  /// Pushes data to workers without forwarding it to peers.
  void local_push(data_message x) {
    BROKER_TRACE(BROKER_ARG(x)
                 << BROKER_ARG2("num_paths", worker_manager().num_paths()));
    if (worker_manager().num_paths() > 0) {
      worker_manager().push(std::move(x));
      worker_manager().emit_batches();
    }
  }

  /// Pushes data to stores without forwarding it to peers.
  void local_push(command_message x) {
    BROKER_TRACE(BROKER_ARG(x)
                 << BROKER_ARG2("num_paths", store_manager().num_paths()));
    if (store_manager().num_paths() > 0) {
      store_manager().push(std::move(x));
      store_manager().emit_batches();
    }
  }

  /// Pushes data to peers only without forwarding it to local substreams.
  void remote_push(message_type msg) {
    BROKER_TRACE(BROKER_ARG(msg));
    if (recorder_ != nullptr)
      try_record(msg);
    peer_manager().push(std::move(msg));
    peer_manager().emit_batches();
  }

  using caf::stream_manager::push;

  /// Pushes data to peers and workers.
  void push(data_message msg) {
    BROKER_TRACE(BROKER_ARG(msg));
    remote_push(make_node_message(std::move(msg), dref().options.ttl));
    // local_push(std::move(x), std::move(y));
  }

  /// Pushes data to peers and stores.
  void push(command_message msg) {
    BROKER_TRACE(BROKER_ARG(msg));
    remote_push(make_node_message(std::move(msg), dref().options.ttl));
    // local_push(std::move(x), std::move(y));
  }

  // -- overridden member functions of caf::stream_manager ---------------------

  void handle_batch(const caf::strong_actor_ptr& hdl, caf::message& xs) {
    BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(xs));
    // If there's anything in the central buffer at this point, it's stuff that
    // we're sending out ourselves (as opposed to forwarding), so we flush it
    // out to each path's own cache now to make sure the subsequent flush in
    // after_handle_batch doesn't accidentally filter out messages where the
    // outband path of previously-buffered messagesi happens to match the path
    // of the inbound data we are handling here.
    BROKER_ASSERT(peer_manager().selector().active_sender == nullptr);
    peer_manager().fan_out_flush();
    peer_manager().selector().active_sender = caf::actor_cast<caf::actor_addr>(hdl);
    auto guard = caf::detail::make_scope_guard([this] {
      // Make sure the content of the buffer is pushed to the outbound paths
      // while the sender filter is still active.
      peer_manager().fan_out_flush();
      peer_manager().selector().active_sender = nullptr;
    });
    // Handle received batch.
    if (xs.match_elements<typename peer_trait::batch>()) {
      auto peer_actor = caf::actor_cast<caf::actor>(hdl);
      auto it = blocked_peers.find(peer_actor);
      if (it != blocked_peers.end()) {
        BROKER_DEBUG("buffer batch from blocked peer" << hdl);
        auto& bmsgs = blocked_msgs[peer_actor];
        bmsgs.emplace_back(std::move(xs));
        return;
      }
      auto num_workers = worker_manager().num_paths();
      auto num_stores = store_manager().num_paths();
      BROKER_DEBUG("forward batch from peers;" << BROKER_ARG(num_workers)
                                               << BROKER_ARG(num_stores));
      // Only received from other peers. Extract content for to local workers
      // or stores and then forward to other peers.
      for (auto& msg : xs.get_mutable_as<typename peer_trait::batch>(0)) {
        const topic* t;
        // Dispatch to local workers or stores messages.
        if (is_data_message(msg)) {
          auto& dm = get<data_message>(msg.content);
          t = &get_topic(dm);
          if (num_workers > 0)
            worker_manager().push(dm);
        } else {
          auto& cm = get<command_message>(msg.content);
          t = &get_topic(cm);
          if (num_stores > 0)
            store_manager().push(cm);
        }
        // Check if forwarding is on.
        if (!dref().options.forward)
          continue;
        // Somewhat hacky, but don't forward data store clone messages.
        auto ends_with = [](const std::string& s, const std::string& ending) {
          if (ending.size() > s.size())
            return false;
          return std::equal(ending.rbegin(), ending.rend(), s.rbegin());
        };
        if (ends_with(t->string(), topics::clone_suffix.string()))
          continue;
        // Either decrease TTL if message has one already, or add one.
        if (--msg.ttl == 0) {
          BROKER_WARNING("dropped a message with expired TTL");
          continue;
        }
        // Forward to other peers.
        peer_manager().push(std::move(msg));
      }
      return;
    }
    using variant_batch = std::vector<node_message::value_type>;
    if (try_handle<worker_trait>(xs, "publish from local workers")
        || try_handle<store_trait>(xs, "publish from local stores")
        || try_handle<var_trait>(xs, "publish from custom actors"))
      return;
    BROKER_ERROR("unexpected batch:" << deep_to_string(xs));
  }

  void handle(caf::inbound_path* path,
              caf::downstream_msg::batch& batch) override {
    handle_batch(path->hdl, batch.xs);
  }

  void handle(caf::inbound_path* path, caf::downstream_msg::close& x) override {
    BROKER_TRACE(BROKER_ARG(path) << BROKER_ARG(x));
    auto slot = path->slots.receiver;
    remove_cb(slot, istream_to_hdl_, hdl_to_istream_, hdl_to_ostream_, caf::none);
  }

  void handle(caf::inbound_path* path,
              caf::downstream_msg::forced_close& x) override {
    BROKER_TRACE(BROKER_ARG(path) << BROKER_ARG(x));
    auto slot = path->slots.receiver;
    remove_cb(slot, istream_to_hdl_, hdl_to_istream_, hdl_to_ostream_,
              std::move(x.reason));
  }

  void handle(caf::stream_slots slots, caf::upstream_msg::drop& x) override {
    BROKER_TRACE(BROKER_ARG(slots) << BROKER_ARG(x));
    caf::stream_manager::handle(slots, x);
  }

  void handle(caf::stream_slots slots,
              caf::upstream_msg::forced_drop& x) override {
    BROKER_TRACE(BROKER_ARG(slots) << BROKER_ARG(x));
    auto slot = slots.receiver;
    if (out_.remove_path(slots.receiver, x.reason, true))
      remove_cb(slot, ostream_to_peer_, hdl_to_ostream_, hdl_to_istream_,
                std::move(x.reason));
  }

  bool handle(caf::stream_slots slots,
              caf::upstream_msg::ack_open& x) override {
    BROKER_TRACE(BROKER_ARG(slots) << BROKER_ARG(x));
    auto rebind_from = x.rebind_from;
    auto rebind_to = x.rebind_to;
    if (caf::stream_manager::handle(slots, x)) {
      ack_open_success(slots.receiver, rebind_from, rebind_to);
      return true;
    }
    ack_open_failure(slots.receiver, rebind_from, rebind_to);
    return false;
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

  /// Applies `f` to each peer.
  template <class F>
  void for_each_peer(F f) {
    // visit all peers that have at least one path still connected
    auto peers = get_peer_handles();
    std::for_each(peers.begin(), peers.end(), std::move(f));
  }

  /// Returns all known peers.
  auto get_peer_handles() {
    std::vector<caf::actor> peers;
    for (auto& kvp : hdl_to_ostream_)
      peers.emplace_back(kvp.first);
    for (auto& kvp : hdl_to_istream_)
      peers.emplace_back(kvp.first);
    auto b = peers.begin();
    auto e = peers.end();
    std::sort(b, e);
    auto p = std::unique(b, e);
    if (p != e)
      peers.erase(p, e);
    return peers;
  }

  /// Finds the first peer handle that satisfies the predicate.
  template <class Predicate>
  caf::actor find_output_peer_hdl(Predicate pred) {
    for (auto& kvp : hdl_to_ostream_)
      if (pred(kvp.first))
        return kvp.first;
    return nullptr;
  }

  /// Applies `f` to each filter.
  template <class F>
  void for_each_filter(F f) {
    for (auto& kvp : peer_manager().states()) {
      f(kvp.second.filter);
    }
  }

protected:
  /// @pre `recorder_ != nullptr`
  template <class T>
  bool try_record(const T& x) {
    BROKER_ASSERT(recorder_ != nullptr);
    BROKER_ASSERT(remaining_records_ > 0);
    if (auto err = recorder_->write(x)) {
      BROKER_WARNING("unable to write to generator file:" << err);
      recorder_ = nullptr;
      remaining_records_ = 0;
      return false;
    }
    if (--remaining_records_ == 0) {
      BROKER_DEBUG("reached recording cap, close file");
      recorder_ = nullptr;
    }
    return true;
  }

  bool try_record(const node_message& x) {
    return try_record(x.content);
  }

  template <class Trait>
  bool try_handle(caf::message& msg, const char* debug_msg) {
    CAF_IGNORE_UNUSED(debug_msg);
    using batch_type = typename Trait::batch;
    if (msg.match_elements<batch_type>()) {
      using iterator_type = typename batch_type::iterator;
      auto ttl0 = initial_ttl();
      auto push_unrecorded = [&](iterator_type first, iterator_type last) {
        for (auto i = first; i != last; ++i)
          peer_manager().push(make_node_message(std::move(*i), ttl0));
      };
      auto push_recorded = [&](iterator_type first, iterator_type last) {
        for (auto i = first; i != last; ++i) {
          if (!try_record(*i))
            return i;
          peer_manager().push(make_node_message(std::move(*i), ttl0));
        }
        return last;
      };
      BROKER_DEBUG(debug_msg);
      auto& xs = msg.get_mutable_as<batch_type>(0);
      if (recorder_ == nullptr) {
        push_unrecorded(xs.begin(), xs.end());
      } else {
        auto n = std::min(remaining_records_, xs.size());
        auto first = xs.begin();
        auto last = xs.end();
        auto i = push_recorded(first, first + n);
        if (i != last)
          push_unrecorded(i, last);
      }
      return true;
    }
    return false;
  }

  /// Returns the initial TTL value when publishing data.
  ttl initial_ttl() {
    return static_cast<ttl>(dref().options.ttl);
  }

  /// Adds entries to `hdl_to_istream_` and `istream_to_hdl_`.
  void add_ipath(caf::stream_slot slot, const caf::actor& peer_hdl) {
    BROKER_TRACE(BROKER_ARG(slot) << BROKER_ARG(peer_hdl));
    if (slot == caf::invalid_stream_slot) {
      BROKER_ERROR("tried to add an invalid inbound path");
      return;
    }
    if (!istream_to_hdl_.emplace(slot, peer_hdl).second) {
      BROKER_ERROR("ipath_to_peer entry already exists");
      return;
    }
    if (!hdl_to_istream_.emplace(peer_hdl, slot).second) {
      BROKER_ERROR("peer_to_ipath entry already exists");
      return;
    }
  }

  /// Adds entries to `hdl_to_ostream_` and `ostream_to_peer_`.
  void add_opath(caf::stream_slot slot, const caf::actor& peer_hdl) {
    BROKER_TRACE(BROKER_ARG(slot) << BROKER_ARG(peer_hdl));
    if (slot == caf::invalid_stream_slot) {
      BROKER_ERROR("tried to add an invalid outbound path");
      return;
    }
    if (!ostream_to_peer_.emplace(slot, peer_hdl).second) {
      BROKER_ERROR("opath_to_peer entry already exists");
      return;
    }
    if (!hdl_to_ostream_.emplace(peer_hdl, slot).second) {
      BROKER_ERROR("peer_to_opath entry already exists");
      return;
    }
  }

  /// Path `slot` in `xs` was dropped or closed. Removes the entry in `xs` as
  /// well as the associated entry in `ys`. Also removes the entries from `as`
  /// and `bs` if `reason` is not default constructed. Calls `remove_peer` if
  /// no entry for a peer exists afterwards.
  void remove_cb(caf::stream_slot slot, slot_to_hdl_map& xs,
                 hdl_to_slot_map& ys, hdl_to_slot_map& zs, caf::error reason) {
    BROKER_TRACE(BROKER_ARG(slot));
    auto i = xs.find(slot);
    if (i == xs.end()) {
      BROKER_DEBUG("no entry in xs found for slot" << slot);
      return;
    }
    auto peer_hdl = i->second;
    remove_peer(peer_hdl, std::move(reason), true, false);
  }

  /// Sends a handshake with filter in step #1.
  step1_handshake add(std::true_type send_own_filter, const caf::actor& hdl) {
    auto xs
      = std::make_tuple(dref().get_all_filter(hdl), caf::actor_cast<caf::actor>(self()));
    return this->template add_unchecked_outbound_path<node_message>(
      hdl, std::move(xs));
  }

  /// Sends a handshake with 'ok' in step #2.
  step2_handshake add(std::false_type send_own_filter, const caf::actor& hdl) {
    atom_value ok = caf::ok_atom::value;
    auto xs = std::make_tuple(ok, caf::actor_cast<caf::actor>(self()));
    return this->template add_unchecked_outbound_path<node_message>(
      hdl, std::move(xs));
  }

  /// Organizes downstream communication to peers as well as local subscribers.
  downstream_manager_type out_;

  /// Maps peer handles to output path IDs.
  hdl_to_slot_map hdl_to_ostream_;

  /// Maps output path IDs to peer handles.
  slot_to_hdl_map ostream_to_peer_;

  /// Maps peer handles to input path IDs.
  hdl_to_slot_map hdl_to_istream_;

  /// Maps input path IDs to peer handles.
  slot_to_hdl_map istream_to_hdl_;

  /// Peers that are currently blocked (messages buffered until unblocked).
  std::unordered_set<caf::actor> blocked_peers;

  /// Messages that are currently buffered.
  std::unordered_map<caf::actor, std::vector<caf::message>> blocked_msgs;

  /// Maps pending peer handles to output IDs. An invalid stream ID indicates
  /// that only "step #0" was performed so far. An invalid stream ID corresponds
  /// to `peer_status::connecting` and a valid stream ID cooresponds to
  /// `peer_status::connected`. The status for a given handle `x` is
  /// `peer_status::peered` if `governor->has_peer(x)` returns true.
  std::unordered_map<caf::actor, pending_connection> pending_connections_;

  /// Helper for recording meta data of published messages.
  detail::generator_file_writer_ptr recorder_;

  /// Counts down when using a `recorder_` to cap maximum file entries.
  size_t remaining_records_;

private:
  Derived& dref() {
    return static_cast<Derived&>(*this);
  }
};

} // namespace broker::alm
