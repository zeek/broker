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

  core_manager(caf::node_id core_id, endpoint::clock* clock,
               caf::event_based_actor* self, const domain_options* adaptation);

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
  ~core_state();

  caf::intrusive_ptr<core_manager> mgr;

  static inline const char* name = "core";
};

using core_actor_type = caf::stateful_actor<core_state>;

// We wrap the factory function for the core actor to enable spawn without
// passing values for parameters that provide a default value. With a plain old
// function, CAF receives a function pointer that requires the call side to pass
// in all parameters.

/// Function object wrapper for the core actor implementation.
struct core_actor_t {
  /// @param self Pointer to the newly created core actor.
  /// @param initial_filter Pre-subscribed topics.
  /// @param clock Optional pointer to a non-real-time clock. Allows Zeek to
  ///              advance time based on timestamps of recorded traffic.
  /// @param adaptation Optional adaptation that extends the system-wide
  ///                   configuration. The adaptation overrides settings that
  ///                   also exist as system-wide parameters.
  caf::behavior operator()(core_actor_type* self, filter_type initial_filter,
                           endpoint::clock* clock = nullptr,
                           const domain_options* adaptation = nullptr) const;
};

/// Function object implementing the core actor.
constexpr auto core_actor = core_actor_t{};

} // namespace broker
