#pragma once

#include <unordered_set>

#include <caf/actor.hpp>
#include <caf/behavior.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/event_based_actor.hpp>

#include "broker/data.hh"
#include "broker/detail/store_actor.hh"
#include "broker/endpoint.hh"
#include "broker/fwd.hh"
#include "broker/internal_command.hh"
#include "broker/publisher_id.hh"
#include "broker/topic.hh"

namespace broker {
namespace detail {

class abstract_backend;

class master_state : public store_actor_state {
public:
  using super = store_actor_state;

  /// Owning smart pointer to a backend.
  using backend_pointer = std::unique_ptr<abstract_backend>;

  /// Initializes the object.
  void init(caf::event_based_actor* ptr, std::string&& nm,
            backend_pointer&& bp, caf::actor&& parent, endpoint::clock* clock);

  /// Sends `x` to all clones.
  void broadcast(internal_command&& x);

  template <class T>
  void broadcast_cmd_to_clones(T cmd) {
    BROKER_DEBUG("broadcast" << cmd << "to" << clones.size() << "clones");
    if (!clones.empty())
      broadcast(internal_command{std::move(cmd)});
  }

  void remind(timespan expiry, const data& key);

  void expire(data& key);

  void command(internal_command& cmd);

  void command(internal_command::variant_type& cmd);

  void operator()(none);

  void operator()(put_command&);

  void operator()(put_unique_command&);

  void operator()(erase_command&);

  void operator()(expire_command&);

  void operator()(add_command&);

  void operator()(subtract_command&);

  void operator()(snapshot_command&);

  void operator()(snapshot_sync_command&);

  void operator()(set_command&);

  void operator()(clear_command&);

  topic clones_topic;

  backend_pointer backend;

  std::unordered_map<caf::actor_addr, caf::actor> clones;

  bool exists(const data& key);

  static inline constexpr const char* name = "master_actor";
};

caf::behavior master_actor(caf::stateful_actor<master_state>* self,
                           caf::actor core, std::string id,
                           master_state::backend_pointer backend,
                           endpoint::clock* clock);

} // namespace detail
} // namespace broker
