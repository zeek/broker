#include "broker/detail/stream_governor.hh"

#include "caf/event_based_actor.hpp"
#include "caf/policy/broadcast.hpp"
#include "caf/policy/greedy.hpp"

#include "broker/detail/core_actor.hh"

namespace broker {
namespace detail {

stream_governor::stream_governor(core_state* state)
    : state_(state),
      in_(state->self, caf::policy::greedy::make()),
      local_subscribers_(state->self, state->sid,
                         caf::policy::broadcast::make()) {
  // nop
}

stream_governor::peer_data* stream_governor::add_peer(caf::strong_actor_ptr hdl,
                                                      filter_type filter) {
  CAF_LOG_TRACE(CAF_ARG(hdl) << CAF_ARG(filter));
  auto pp = caf::policy::broadcast::make();
  auto ptr = new peer_data{std::move(filter), state_->self,
                          state_->sid, std::move(pp)};
  ptr->out.add_path(hdl);
  auto res = peers_.emplace(std::move(hdl), peer_data_ptr{ptr});
  return res.second ? ptr : nullptr;
}

caf::error stream_governor::add_downstream(caf::strong_actor_ptr&) {
  CAF_LOG_ERROR("add_downstream on governor called");
  return caf::sec::invalid_stream_state;
}

caf::error
stream_governor::confirm_downstream(const caf::strong_actor_ptr& rebind_from,
                                    caf::strong_actor_ptr& hdl,
                                    long initial_demand, bool redeployable) {
  CAF_LOG_TRACE(CAF_ARG(rebind_from) << CAF_ARG(hdl)
                << CAF_ARG(initial_demand) << CAF_ARG(redeployable));
  CAF_IGNORE_UNUSED(redeployable);
  auto path = local_subscribers_.find(rebind_from);
  if (path) {
    if (!local_subscribers_.confirm_path(rebind_from, hdl, initial_demand)) {
      CAF_LOG_ERROR("Cannot rebind to registered downstream.");
      return caf::sec::invalid_stream_state;
    }
    return downstream_demand(hdl, initial_demand);
  }
  auto i = peers_.find(rebind_from);
  if (i != peers_.end()) {
    auto uptr = std::move(i->second);
    peers_.erase(i);
    auto res = peers_.emplace(hdl, std::move(uptr));
    if (!res.second) {
      CAF_LOG_ERROR("Cannot rebind to registered downstream.");
      return caf::sec::invalid_stream_state;
    }
    CAF_LOG_DEBUG("Confirmed path to another core"
                 << CAF_ARG(rebind_from) << CAF_ARG(hdl));
    res.first->second->out.confirm_path(rebind_from, hdl, initial_demand);
    return downstream_demand(hdl, initial_demand);
  }
  CAF_LOG_ERROR("Cannot confirm path to unknown downstream.");
  return caf::sec::invalid_downstream;
}

caf::error stream_governor::downstream_demand(caf::strong_actor_ptr& hdl,
                                              long value) {
  CAF_LOG_TRACE(CAF_ARG(hdl) << CAF_ARG(value));
  auto path = local_subscribers_.find(hdl);
  if (path) {
    path->open_credit += value;
    return push(nullptr);
  }
  auto i = peers_.find(hdl);
  if (i != peers_.end()) {
    auto pp = i->second->out.find(hdl);
    if (!pp)
      return caf::sec::invalid_stream_state;
    CAF_LOG_DEBUG("grant" << value << "new credit to" << hdl);
    pp->open_credit += value;
    return push(nullptr);
  }
  return caf::sec::invalid_downstream;
}

caf::error stream_governor::push(long* hint) {
  CAF_LOG_TRACE("");
  if (local_subscribers_.buf_size() > 0)
    local_subscribers_.policy().push(local_subscribers_, hint);
  for (auto& kvp : peers_) {
    auto& out = kvp.second->out;
    if (out.buf_size() > 0)
      out.policy().push(out, hint);
  }
  return caf::none;
}

caf::expected<long> stream_governor::add_upstream(caf::strong_actor_ptr& hdl,
                                                  const caf::stream_id& sid,
                                                  caf::stream_priority prio) {
  CAF_LOG_TRACE(CAF_ARG(hdl) << CAF_ARG(sid) << CAF_ARG(prio));
  if (hdl)
    return in_.add_path(hdl, sid, prio, total_downstream_net_credit());
  return caf::sec::invalid_argument;
}

caf::error stream_governor::upstream_batch(caf::strong_actor_ptr& hdl,
                                           long xs_size, caf::message& xs) {
  CAF_LOG_TRACE(CAF_ARG(hdl) << CAF_ARG(xs_size) << CAF_ARG(xs));
  using std::get;
  // Sanity checking.
  auto path = in_.find(hdl);
  if (!path)
    return caf::sec::invalid_upstream;
  if (xs_size > path->assigned_credit)
    return caf::sec::invalid_stream_state;
  if (!xs.match_elements<std::vector<element_type>>())
    return caf::sec::unexpected_message;
  // Unwrap `xs`.
  auto& vec = xs.get_mutable_as<std::vector<element_type>>(0);
  // Decrease credit assigned to `hdl` and get currently available downstream
  // credit on all paths.
  CAF_LOG_DEBUG(CAF_ARG(path->assigned_credit) << CAF_ARG(xs_size));
  path->assigned_credit -= xs_size;
  // Forward data to all other peers.
  auto selected = [](const filter_type& f, const element_type& x) -> bool {
    using std::get;
    for (auto& key : f)
      if (key == get<0>(x))
        return true;
    return false;
  };
  for (auto& kvp : peers_)
    if (kvp.first != hdl) {
      auto& out = kvp.second->out;
      for (const auto& x : vec)
        if (selected(kvp.second->filter, x))
          out.push(x);
      if (out.buf_size() > 0) {
        out.policy().push(out);
      }
    }
  // Move elements from `xs` to the buffer for local subscribers.
  for (auto& x : vec)
    local_subscribers_.push(std::move(x));
  local_subscribers_.policy().push(local_subscribers_);
  // Grant new credit to upstream if possible.
  auto available = total_downstream_net_credit();
  if (available > 0)
    in_.assign_credit(available);
  return caf::none;
}

caf::error stream_governor::close_upstream(caf::strong_actor_ptr& hdl) {
  CAF_LOG_TRACE(CAF_ARG(hdl));
  if (in_.remove_path(hdl))
    return caf::none;
  return caf::sec::invalid_upstream;
}

void stream_governor::abort(caf::strong_actor_ptr& hdl,
                            const caf::error& reason) {
  CAF_LOG_TRACE(CAF_ARG(hdl) << CAF_ARG(reason));
  CAF_IGNORE_UNUSED(reason);
  if (local_subscribers_.remove_path(hdl))
    return;
  auto i = peers_.find(hdl);
  if (i != peers_.end()) {
    auto& pd = *i->second;
    state_->self->streams().erase(pd.incoming_sid);
    peers_.erase(i);
  }
}

bool stream_governor::done() const {
  return false;
}

caf::message stream_governor::make_output_token(const caf::stream_id& x) const {
  return caf::make_message(caf::stream<element_type>{x});
}

long stream_governor::total_downstream_net_credit() const {
  auto net_credit = local_subscribers_.total_net_credit();
  for (auto& kvp : peers_)
    net_credit = std::min(net_credit, kvp.second->out.total_net_credit());
  return net_credit;
}

void stream_governor::new_stream(const caf::strong_actor_ptr& hdl,
                                 const stream_type& token,
                                 caf::message msg) {
  CAF_LOG_TRACE(CAF_ARG(hdl) << CAF_ARG(token) << CAF_ARG(msg));
  CAF_ASSERT(hdl != nullptr);
  auto self = state_->self;
  hdl->enqueue(
    caf::make_mailbox_element(self->ctrl(), caf::message_id::make(), {},
                              caf::make<caf::stream_msg::open>(
                                token.id(), std::move(msg), self->ctrl(), hdl,
                                caf::stream_priority::normal, false)),
    self->context());
  self->streams().emplace(token.id(), this);
}

} // namespace detail
} // namespace broker
