#include "broker/core_actor.hh"

namespace broker {

caf::behavior core_manager::make_behavior() {
  return super::make_behavior(
    [=](atom::get, atom::peer) {
      std::vector<peer_info> result;
      // Add all direct connections from the routing table.
      alm::for_each_direct(tbl(), [&, this](const auto& id, const auto& hdl) {
        endpoint_info ep{id, cache().find(hdl)};
        result.push_back(
          {std::move(ep), peer_flags::remote, peer_status::peered});
      });
      // Add all pending peerings from the stream transport.
      for (const auto& [peer_id, pending_conn] : pending_connections()) {
        endpoint_info ep{peer_id, cache().find(pending_conn.hdl)};
        result.push_back(
          {std::move(ep), peer_flags::remote, peer_status::connected});
      }
      return result;
    });
}

caf::behavior core_actor(core_actor_type* self, filter_type initial_filter,
                         broker_options options, endpoint::clock* clock) {
  auto& mgr = self->state.mgr;
  mgr = caf::make_counted<core_manager>(clock, self);
  if (!initial_filter.empty())
    mgr->subscribe(initial_filter);
  mgr->cache().set_use_ssl(not options.disable_ssl);
  self->set_exit_handler([self](caf::exit_msg& msg) {
    if (msg.reason) {
      BROKER_DEBUG("shutting down after receiving an exit message with reason:"
                   << msg.reason);
      self->quit(std::move(msg.reason));
    }
  });
  return mgr->make_behavior();
}

} // namespace broker
