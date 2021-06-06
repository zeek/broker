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
class core_state : public                              //
                   mixin::notifier<                    //
                     mixin::connector<                 //
                       mixin::data_store_manager<      //
                         mixin::recorder<              //
                           alm::stream_transport>>>> { //
public:
  using super = extended_base;

  static inline const char* name = "broker.core";

  explicit core_state(caf::event_based_actor* self, endpoint_id this_peer,
                      filter_type initial_filter,
                      endpoint::clock* clock = nullptr,
                      const domain_options* adaptation = nullptr);

  ~core_state() override;

  caf::behavior make_behavior() override;
};

using core_actor_type = caf::stateful_actor<core_state>;

} // namespace broker
