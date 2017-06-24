#include "broker/detail/stream_relay.hh"

#include "broker/detail/stream_governor.hh"

namespace broker {
namespace detail {

stream_relay::stream_relay(caf::intrusive_ptr<stream_governor> gov,
                           const caf::stream_id& sid, token_factory factory)
  : governor_(std::move(gov)),
    sid_(std::move(sid)),
    factory_(std::move(factory)) {
  // nop
}

stream_relay::~stream_relay() {
  // nop
}

caf::error stream_relay::add_downstream(caf::strong_actor_ptr& hdl) {
  return governor_->add_downstream(sid_, hdl);
}

caf::error
stream_relay::confirm_downstream(const caf::strong_actor_ptr& rebind_from,
                                 caf::strong_actor_ptr& hdl,
                                 long initial_demand, bool redeployable) {
  return governor_->confirm_downstream(sid_, rebind_from, hdl, initial_demand,
                                       redeployable);
}

caf::error stream_relay::downstream_ack(caf::strong_actor_ptr& hdl,
                                        int64_t batch_id, long new_demand) {
  return governor_->downstream_ack(sid_, hdl, batch_id, new_demand);
}

caf::error stream_relay::push() {
  return governor_->push();
}

caf::expected<long> stream_relay::add_upstream(caf::strong_actor_ptr& hdl,
                                               const caf::stream_id& sid,
                                               caf::stream_priority prio) {
  return governor_->add_upstream(sid_, hdl, sid, prio);
}

caf::error stream_relay::upstream_batch(caf::strong_actor_ptr& hdl,
                                        int64_t xs_id, long xs_size,
                                        caf::message& xs) {
  return governor_->upstream_batch(sid_, hdl, xs_id, xs_size, xs);
}

caf::error stream_relay::close_upstream(caf::strong_actor_ptr& hdl) {
  return governor_->close_upstream(sid_, hdl);
}

void stream_relay::abort(caf::strong_actor_ptr& hdl,
                         const caf::error& reason) {
  return governor_->abort(sid_, hdl, reason);
}

bool stream_relay::done() const {
  return false;
}

caf::message stream_relay::make_output_token(const caf::stream_id& x) const {
  return factory_(x);
}

} // namespace broker
} // namespace detail
