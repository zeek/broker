#pragma once

#include <caf/make_counted.hpp>

#include "broker/alm/peer.hh"
#include "broker/alm/stream_transport.hh"
#include "broker/mixin/connector.hh"
#include "broker/mixin/data_store_manager.hh"
#include "broker/mixin/notifier.hh"
#include "broker/mixin/recorder.hh"

namespace broker {


/// The core registers these message handlers:
///
/// ~~~
/// (atom::publish, endpoint_info receiver, data_message msg) -> void
/// => ship(msg, receiver.node)
/// ~~~
class core_manager
  : public caf::extend<alm::stream_transport<core_manager, caf::node_id>,
                       core_manager>:: //
    with<mixin::connector, mixin::data_store_manager, mixin::notifier,
         mixin::recorder> {
public:
  using super = extended_base;

  core_manager(endpoint::clock* clock, caf::event_based_actor* self)
    : super(clock, self), id_(self->node()) {
    // nop
  }

  const auto& id() const noexcept {
    return id_;
  }

  /// Overrides the peer ID (use for testing only).
  /// @private
  void id(caf::node_id id) {
    id_ = std::move(id);
  }

  caf::behavior make_behavior();

private:
  caf::node_id id_;
};

struct core_state {
  caf::intrusive_ptr<core_manager> mgr;

  static inline const char* name = "core";
};

using core_actor_type = caf::stateful_actor<core_state>;

caf::behavior core_actor(core_actor_type* self, filter_type initial_filter,
                         broker_options opts, endpoint::clock* clock);

} // namespace broker
