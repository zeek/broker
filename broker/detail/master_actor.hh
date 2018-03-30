#ifndef BROKER_DETAIL_MASTER_ACTOR_HH
#define BROKER_DETAIL_MASTER_ACTOR_HH

#include <unordered_set>

#include <caf/stateful_actor.hpp>

#include "broker/data.hh"
#include "broker/fwd.hh"
#include "broker/internal_command.hh"
#include "broker/topic.hh"
#include "broker/endpoint.hh"

namespace broker {
namespace detail {

class abstract_backend;

class master_state {
public:
  /// Allows us to apply this state as a visitor to internal commands.
  using result_type = void;

  /// Owning smart pointer to a backend.
  using backend_pointer = std::unique_ptr<abstract_backend>;

  /// Creates an uninitialized object.
  master_state();

  /// Initializes the object.
  void init(caf::event_based_actor* ptr, std::string&& nm,
            backend_pointer&& bp, caf::actor&& parent, endpoint* ep);

  /// Sends `x` to all clones.
  void broadcast(internal_command&& x);

  template <class T>
  void broadcast_cmd_to_clones(T cmd) {
    if (!clones.empty())
      broadcast(internal_command{std::move(cmd)});
  }

  void remind(timespan expiry, const data& key);

  void expire(data& key);

  void command(internal_command& cmd);

  void operator()(none);

  void operator()(put_command&);

  void operator()(put_unique_command&);

  void operator()(erase_command&);

  void operator()(add_command&);

  void operator()(subtract_command&);

  void operator()(snapshot_command&);

  void operator()(set_command&);

  void operator()(clear_command&);

  caf::event_based_actor* self;

  std::string id;

  topic clones_topic;

  backend_pointer backend;

  caf::actor core;

  std::unordered_set<caf::actor_addr> clones;

  static const char* name;

  endpoint* ep;
};

caf::behavior master_actor(caf::stateful_actor<master_state>* self,
                           caf::actor core, std::string id,
                           master_state::backend_pointer backend,
                           endpoint* ep);

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_MASTER_ACTOR_HH
