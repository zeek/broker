#include "broker/detail/core_policy.hh"

#include <caf/fused_scatterer.hpp>
#include <caf/random_gatherer.hpp>

#include <caf/detail/stream_distribution_tree.hpp>

#include "broker/detail/core_actor.hh"

using caf::detail::stream_distribution_tree;

using namespace caf;

namespace broker {
namespace detail {

core_policy::core_policy(stream_distribution_tree<core_policy>* parent,
                         core_state* state, filter_type)
  : parent_(parent),
    state_(state) {
  // nop
}

bool core_policy::at_end() const {
  return shutting_down() && peers().paths_clean()
         && workers().paths_clean() && stores().paths_clean();
}

bool core_policy::substream_local_data() const {
  return false;
}

void core_policy::before_handle_batch(const stream_id&,
                                      const actor_addr& hdl, long,
                                      message&, int64_t) {
  parent_->out().main_stream().selector().active_sender = hdl;
}

void core_policy::handle_batch(message& xs) {
  if (xs.match_elements<peer_batch>()) {
    // Only received from other peers. Extract content for to local workers
    // or stores and then forward to other peers.
    for (auto& msg : xs.get_mutable_as<peer_batch>(0)) {
      // Extract worker messages.
      if (msg.match_elements<topic, data>())
      {
        workers().push(msg.get_as<topic>(0), msg.get_as<data>(1));
      }
      // Extract store messages.
      if (msg.match_elements<topic, internal_command>())
        stores().push(msg.get_as<topic>(0), msg.get_as<internal_command>(1));
      // Forward to other peers.
      peers().push(std::move(msg));
    }
    return;
  }
  if (xs.match_elements<worker_batch>()) {
    // Inputs from local workers are only forwarded to peers.
    for (auto& x : xs.get_mutable_as<worker_batch>(0)) {
      parent_->out().push(make_message(std::move(x.first),
                                       std::move(x.second)));
    }
    return;
  }
  if (xs.match_elements<store_batch>()) {
    // Inputs from stores are only forwarded to peers.
    for (auto& x : xs.get_mutable_as<store_batch>(0)) {
      parent_->out().push(make_message(std::move(x.first),
                                       std::move(x.second)));
    }
    return;
  }
  CAF_LOG_ERROR("unexpected batch:" << deep_to_string(xs));
}

void core_policy::after_handle_batch(const stream_id&, const actor_addr&,
                                     int64_t) {
  parent_->out().main_stream().selector().active_sender = nullptr;
}

void core_policy::ack_open_success(const stream_id& sid,
                                   const actor_addr& rebind_from,
                                   strong_actor_ptr rebind_to) {
  auto old_id = std::make_pair(sid, rebind_from);
  auto new_id = std::make_pair(sid, actor_cast<actor_addr>(rebind_to));
  auto i = opath_to_peer_.find(old_id);
  if (i != opath_to_peer_.end()) {
    auto pp = std::move(i->second);
    peer_to_opath_[pp] = new_id;
    opath_to_peer_.erase(i);
    opath_to_peer_.emplace(new_id, std::move(pp));
  }
}

void core_policy::ack_open_failure(const stream_id& sid,
                                   const actor_addr& rebind_from,
                                   strong_actor_ptr rebind_to, const error&) {
  auto old_id = std::make_pair(sid, rebind_from);
  auto new_id = std::make_pair(sid, actor_cast<actor_addr>(rebind_to));
  auto i = opath_to_peer_.find(old_id);
  if (i != opath_to_peer_.end()) {
    auto pp = std::move(i->second);
    peer_lost(pp);
    peer_to_opath_.erase(pp);
    opath_to_peer_.erase(i);
  }
}

void core_policy::push_to_substreams(std::vector<message> vec) {
  // Move elements from `xs` to the buffer for local subscribers.
  if (!workers().lanes().empty())
    for (auto& x : vec)
      if (x.match_elements<topic, data>()) {
        x.force_unshare();
        workers().push(x.get_as<topic>(0),
                      std::move(x.get_mutable_as<data>(1)));
      }
  workers().emit_batches();
  if (!stores().lanes().empty())
    for (auto& x : vec)
      if (x.match_elements<topic, internal_command>()) {
        x.force_unshare();
        stores().push(x.get_as<topic>(0),
                     std::move(x.get_mutable_as<internal_command>(1)));
      }
  stores().emit_batches();
}

optional<error> core_policy::batch(const stream_id&, const actor_addr&, long,
                                   message& xs, int64_t) {
  if (xs.match_elements<std::vector<std::pair<topic, data>>>()) {
    return error{caf::none};
  }
  if (xs.match_elements<std::vector<std::pair<topic, internal_command>>>()) {
    return error{caf::none};
  }
  return caf::none;
}

// -- status updates to the state ----------------------------------------------

void core_policy::peer_lost(const actor& hdl) {
  state_->emit_status<sc::peer_lost>(hdl, "lost remote peer");
}

void core_policy::peer_removed(const caf::actor& hdl) {
  state_->emit_status<sc::peer_removed>(hdl, "removed peering");
}

// -- callbacks for close/drop events ------------------------------------------

error core_policy::path_closed(const stream_id& sid, const actor_addr& hdl) {
  CAF_LOG_TRACE(CAF_ARG(sid) << CAF_ARG(hdl));
  auto path_id = std::make_pair(sid, hdl);
  auto i = ipath_to_peer_.find(path_id);
  if (i == ipath_to_peer_.end())
    return caf::none;
  auto peer_hdl = i->second;
  // Remove peer entirely if no more path to it exists, otherwise only drop
  // output path state.
  if (peer_to_opath_.count(peer_hdl) == 0) {
    remove_peer(peer_hdl, caf::none, true, false);
  } else {
    peer_to_ipath_.erase(peer_hdl);
    ipath_to_peer_.erase(i);
  }
  return caf::none;
}

error core_policy::path_force_closed(const stream_id& sid,
                                     const actor_addr& hdl, error reason) {
  CAF_LOG_TRACE(CAF_ARG(sid) << CAF_ARG(hdl) << CAF_ARG(reason));
  auto path_id = std::make_pair(sid, hdl);
  auto i = ipath_to_peer_.find(path_id);
  if (i == ipath_to_peer_.end())
    return caf::none;
  auto peer_hdl = i->second;
  remove_peer(peer_hdl, std::move(reason), true, false);
  return caf::none;
}

error core_policy::path_dropped(const stream_id& sid, const actor_addr& hdl) {
  CAF_LOG_TRACE(CAF_ARG(sid) << CAF_ARG(hdl));
  auto path_id = std::make_pair(sid, hdl);
  auto i = opath_to_peer_.find(path_id);
  if (i == opath_to_peer_.end())
    return caf::none;
  auto peer_hdl = i->second;
  // Remove peer entirely if no more path to it exists, otherwise only drop
  // input path state.
  if (peer_to_ipath_.count(peer_hdl) == 0) {
    remove_peer(peer_hdl, caf::none, true, false);
  } else {
    peer_to_opath_.erase(peer_hdl);
    opath_to_peer_.erase(i);
  }
  return caf::none;
}

error core_policy::path_force_dropped(const stream_id& sid,
                                      const actor_addr& hdl, error reason) {
  CAF_LOG_TRACE(CAF_ARG(sid) << CAF_ARG(hdl) << CAF_ARG(reason));
  auto path_id = std::make_pair(sid, hdl);
  auto i = opath_to_peer_.find(path_id);
  if (i == opath_to_peer_.end())
    return caf::none;
  auto peer_hdl = i->second;
  remove_peer(peer_hdl, std::move(reason), true, false);
  return caf::none;
}

// -- state required by the distribution tree --------------------------------

bool core_policy::shutting_down() const {
  return state_->shutting_down;
}

void core_policy::shutting_down(bool value) {
  state_->shutting_down = value;
}

// -- peer management --------------------------------------------------------

bool core_policy::has_peer(const actor& hdl) const {
  return peer_to_opath_.count(hdl) != 0;
}

bool core_policy::add_peer(const stream_id& sid,
                           const strong_actor_ptr& downstream_handle,
                           const actor& peer_handle, filter_type filter) {
  CAF_LOG_TRACE(CAF_ARG(sid) << CAF_ARG(downstream_handle)
                << CAF_ARG(peer_handle) << CAF_ARG(filter));
  // Sanity check. Could be upgraded to `CAF_ASSERT`.
  if (peers().find(sid, downstream_handle) == nullptr) {
    CAF_LOG_WARNING("add_peer called before adding state to the stream mgr");
    return false;
  }
  auto path_id = std::make_pair(sid, actor_cast<actor_addr>(downstream_handle));
  if (peer_to_opath_.count(peer_handle) != 0
      || opath_to_peer_.count(path_id) != 0)
    return false;
  peers().set_filter(sid, downstream_handle,
                     {path_id.second, std::move(filter)});
  peer_to_opath_.emplace(peer_handle, path_id);
  opath_to_peer_.emplace(std::move(path_id), std::move(peer_handle));
  return true;
}

bool core_policy::init_peer(const stream_id& sid,
                            const strong_actor_ptr& upstream_handle,
                            const actor& peer_handle) {
  auto upstream_addr = actor_cast<actor_addr>(upstream_handle);
  peer_to_ipath_.emplace(peer_handle, std::make_pair(sid, upstream_addr));
  ipath_to_peer_.emplace(std::make_pair(sid, upstream_addr), peer_handle);
  return true;
}

/// Removes a peer, aborting any stream to & from that peer.
bool core_policy::remove_peer(const actor& hdl, error reason, bool silent,
                              bool graceful_removal) {
  CAF_LOG_TRACE(CAF_ARG(hdl));
  int performed_erases = 0;
  { // lifetime scope of first iterator pair
    auto e = peer_to_opath_.end();
    auto i = peer_to_opath_.find(hdl);
    if (i != e) {
      ++performed_erases;
      auto& opath = i->second;
      peers().remove_path(opath.first, opath.second, reason, silent);
      opath_to_peer_.erase(std::make_pair(i->second.first, i->second.second));
      peer_to_opath_.erase(i);
    }
  }
  { // lifetime scope of second iterator pair
    auto e = peer_to_ipath_.end();
    auto i = peer_to_ipath_.find(hdl);
    if (i != e) {
      ++performed_erases;
      auto& ipath = i->second;
      parent_->in().remove_path(ipath.first, ipath.second, reason, silent);
      ipath_to_peer_.erase(std::make_pair(i->second.first, i->second.second));
      peer_to_ipath_.erase(i);
    }
  }
  if (performed_erases == 0)
    return false;
  if (graceful_removal)
    peer_removed(hdl);
  else
    peer_lost(hdl);
  if (shutting_down() && peer_to_opath_.empty()) {
    // Shutdown when the last peer stops listening.
    parent_->self()->quit(exit_reason::user_shutdown);
  } else {
    // See whether we can make progress without that peer in the mix.
    parent_->in().assign_credit(parent_->out().credit());
    parent_->push();
  }
  return true;
}

/// Updates the filter of an existing peer.
bool core_policy::update_peer(const actor& hdl, filter_type filter) {
  CAF_LOG_TRACE(CAF_ARG(hdl) << CAF_ARG(filter));
  auto e = peer_to_opath_.end();
  auto i = peer_to_opath_.find(hdl);
  if (i == e) {
    CAF_LOG_DEBUG("cannot update filter on unknown peer");
    return false;
  }
  peers().set_filter(i->second.first, i->second.second,
                     {i->second.second, std::move(filter)});
  return true;
}

// -- selectively pushing data into the streams ------------------------------

/// Pushes data to workers without forwarding it to peers.
void core_policy::local_push(topic x, data y) {
  workers().push(std::move(x), std::move(y));
  workers().emit_batches();
}

/// Pushes data to stores without forwarding it to peers.
void core_policy::local_push(topic x, internal_command y) {
  stores().push(std::move(x), std::move(y));
  stores().emit_batches();
}

/// Pushes data to peers only without forwarding it to local substreams.
void core_policy::remote_push(message msg) {
  peers().push(std::move(msg));
  peers().emit_batches();
}

/// Pushes data to peers and workers.
void core_policy::push(topic x, data y) {
  remote_push(make_message(x, y));
  //local_push(std::move(x), std::move(y));
}

/// Pushes data to peers and stores.
void core_policy::push(topic x, internal_command y) {
  remote_push(make_message(x, y));
  //local_push(std::move(x), std::move(y));
}

// -- state accessors --------------------------------------------------------

core_policy::main_stream_t& core_policy::peers() {
  return parent_->out().main_stream();
}

const core_policy::main_stream_t& core_policy::peers() const {
  return parent_->out().main_stream();
}

core_policy::substream_t<data>& core_policy::workers() {
  return parent_->out().substream<1>();
}

const core_policy::substream_t<data>& core_policy::workers() const {
  return parent_->out().substream<1>();
}

core_policy::substream_t<internal_command>& core_policy::stores() {
  return parent_->out().substream<2>();
}

const core_policy::substream_t<internal_command>& core_policy::stores() const {
  return parent_->out().substream<2>();
}

std::vector<caf::actor> core_policy::get_peer_handles() {
  std::vector<caf::actor> peers;
  for (auto& kvp : peer_to_opath_)
    peers.emplace_back(kvp.first);
  for (auto& kvp : peer_to_ipath_)
    peers.emplace_back(kvp.first);
  auto b = peers.begin();
  auto e = peers.end();
  std::sort(b, e);
  auto p = std::unique(b, e);
  if (p != e)
    peers.erase(p, e);
  return peers;
}

} // namespace detail
} // namespace broker
