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
  return governor_ ? governor_->add_downstream(sid_, hdl)
                   : make_error(caf::sec::invalid_stream_state);
}

caf::error
stream_relay::confirm_downstream(const caf::strong_actor_ptr& rebind_from,
                                 caf::strong_actor_ptr& hdl,
                                 long initial_demand, bool redeployable) {
  return governor_ ? governor_->confirm_downstream(
                       sid_, rebind_from, hdl, initial_demand, redeployable)
                   : make_error(caf::sec::invalid_stream_state);
}

caf::error stream_relay::downstream_ack(caf::strong_actor_ptr& hdl,
                                        int64_t batch_id, long new_demand) {
  return governor_ ? governor_->downstream_ack(sid_, hdl, batch_id, new_demand)
                   : make_error(caf::sec::invalid_stream_state);
}

caf::error stream_relay::push() {
  return governor_ ? governor_->push()
                   : make_error(caf::sec::invalid_stream_state);
}

caf::expected<long> stream_relay::add_upstream(caf::strong_actor_ptr& hdl,
                                               const caf::stream_id& sid,
                                               caf::stream_priority prio) {
  return governor_ ? governor_->add_upstream(sid_, hdl, sid, prio)
                   : make_error(caf::sec::invalid_stream_state);
}

caf::error stream_relay::upstream_batch(caf::strong_actor_ptr& hdl,
                                        int64_t xs_id, long xs_size,
                                        caf::message& xs) {
  return governor_ ? governor_->upstream_batch(sid_, hdl, xs_id, xs_size, xs)
                   : make_error(caf::sec::invalid_stream_state);
}

caf::error stream_relay::close_upstream(caf::strong_actor_ptr& hdl) {
  return governor_ ? governor_->close_upstream(sid_, hdl)
                   : make_error(caf::sec::invalid_stream_state);
}

void stream_relay::abort(caf::strong_actor_ptr& hdl,
                         const caf::error& reason) {
  if (governor_)
    governor_->abort(sid_, hdl, reason);
}

bool stream_relay::done() const {
  return governor_ == nullptr;
}

void stream_relay::disable() {
  governor_ = nullptr;
  factory_ = nullptr;
}

caf::message stream_relay::make_output_token(const caf::stream_id& x) const {
  return factory_(x);
}

} // namespace broker
} // namespace detail
