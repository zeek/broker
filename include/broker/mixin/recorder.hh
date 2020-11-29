#pragma once

#include "broker/detail/core_recorder.hh"
#include "broker/filter_type.hh"

namespace broker::mixin {

template <class Base>
class recorder : public Base {
public:
  // -- member types -----------------------------------------------------------

  using super = Base;

  using extended_base = recorder;

  // -- constructors, destructors, and assignment operators --------------------

  template <class... Ts>
  explicit recorder(caf::event_based_actor* self, Ts&&... xs)
    : super(self, std::forward<Ts>(xs)...), rec_(super::self()) {
    // nop
  }

  recorder() = delete;

  recorder(const recorder&) = delete;

  recorder& operator=(const recorder&) = delete;

  // -- overrides --------------------------------------------------------------

  void send(const caf::actor& receiver, atom::publish,
            node_message msg) override {
    if (rec_)
      rec_.try_record(msg);
    super::send(receiver, atom::publish_v, std::move(msg));
  }

  void subscribe(const filter_type& what) override {
    if (rec_)
      rec_.record_subscription(what);
    super::subscribe(what);
  }

  void peer_connected(const endpoint_id& remote_id,
                      const caf::actor& hdl) override {
    if (rec_)
      rec_.record_peer(remote_id);
    super::peer_connected(remote_id, hdl);
  }

private:
  detail::core_recorder rec_;
};

} // namespace broker::mixin
