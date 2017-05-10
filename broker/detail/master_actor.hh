#ifndef BROKER_DETAIL_MASTER_ACTOR_HH
#define BROKER_DETAIL_MASTER_ACTOR_HH

#include <unordered_set>

#include <caf/stateful_actor.hpp>

#include "broker/fwd.hh"
#include "broker/data.hh"

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
            backend_pointer&& bp, caf::actor&& parent);

  /// Sends `x` to all clones.
  void broadcast(data&& x);

  template <class T>
  void broadcast_from(T& x) {
    if (!clones.empty())
      broadcast(make_internal_command<T>(std::move(x)));
  }

  void remind(timespan expiry, const data& key);

  void expire(data& key);

  void command(internal_command& cmd);

  void operator()(none);

  void operator()(detail::put_command&);

  void operator()(detail::erase_command&);

  void operator()(detail::add_command&);

  void operator()(detail::subtract_command&);

  void operator()(detail::snapshot_command&);

  caf::event_based_actor* self;

  std::string name;

  topic clones_topic;

  backend_pointer backend;

  caf::actor core;

  std::unordered_set<caf::actor_addr> clones;
};

caf::behavior master_actor(caf::stateful_actor<master_state>* self,
                           caf::actor core, std::string name,
                           master_state::backend_pointer backend);

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_MASTER_ACTOR_HH
