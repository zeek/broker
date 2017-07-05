#ifndef BROKER_DETAIL_STREAM_RELAY_HH
#define BROKER_DETAIL_STREAM_RELAY_HH

#include <functional>

#include <caf/intrusive_ptr.hpp>
#include <caf/stream_handler.hpp>
#include <caf/stream_id.hpp>

namespace broker {
namespace detail {

class stream_governor;
void intrusive_ptr_add_ref(stream_governor*);
void intrusive_ptr_release(stream_governor* p);

/// A stream relay forwards all received messages to the central governor.
class stream_relay : public caf::stream_handler {
public:
  using token_factory = std::function<caf::message (const caf::stream_id&)>;

  stream_relay(caf::intrusive_ptr<stream_governor> gov,
               const caf::stream_id& sid, token_factory factory);

  ~stream_relay();

  caf::error add_downstream(caf::strong_actor_ptr& hdl) override;

  caf::error confirm_downstream(const caf::strong_actor_ptr& rebind_from,
                                caf::strong_actor_ptr& hdl, long initial_demand,
                                bool redeployable) override;

  caf::error downstream_ack(caf::strong_actor_ptr& hdl, int64_t batch_id,
                            long new_demand) override;

  caf::error push() override;

  caf::expected<long> add_upstream(caf::strong_actor_ptr& hdl,
                                   const caf::stream_id& sid,
                                   caf::stream_priority prio) override;

  caf::error upstream_batch(caf::strong_actor_ptr& hdl, int64_t, long,
                            caf::message& xs) override;

  caf::error close_upstream(caf::strong_actor_ptr& hdl) override;

  void abort(caf::strong_actor_ptr& cause, const caf::error& reason) override;

  bool done() const override;

  /// Mark this stream realy as `done`.
  void disable();

  caf::message make_output_token(const caf::stream_id&) const override;

private:
  caf::intrusive_ptr<stream_governor> governor_;
  caf::stream_id sid_;
  token_factory factory_;
};

using stream_relay_ptr = caf::intrusive_ptr<stream_relay>;

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_STREAM_RELAY_HH
