#ifndef BROKER_DETAIL_CLONE_ACTOR_HH
#define BROKER_DETAIL_CLONE_ACTOR_HH

#include <unordered_map>

#include <caf/actor.hpp>
#include <caf/stateful_actor.hpp>

#include "broker/data.hh"
#include "broker/topic.hh"

namespace broker {
namespace detail {

class clone_state {
public:
  /// Allows us to apply this state as a visitor to internal commands.
  using result_type = void;

  /// Creates an uninitialized object.
  clone_state();

  /// Initializes the object.
  void init(caf::event_based_actor* ptr, std::string&& nm,
            caf::actor&& parent);

  /// Sends `x` to the master.
  void forward(data&& x);

  /// Wraps `x` into a `data` object and forwards it to the master.
  template <class T>
  void forward_from(T& x) {
    forward(make_internal_command<T>(std::move(x)));
  }

  void command(internal_command& cmd);

  void operator()(none);

  void operator()(detail::put_command&);

  void operator()(detail::erase_command&);

  void operator()(detail::add_command&);

  void operator()(detail::subtract_command&);

  void operator()(detail::snapshot_command&);

  void operator()(detail::set_command&);

  caf::event_based_actor* self;

  std::string name;

  topic master_topic;

  caf::actor core;

  std::unordered_map<data, data> store;
};

caf::behavior clone_actor(caf::stateful_actor<clone_state>* self,
                          caf::actor core, caf::actor master,
                          std::string name);

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_CLONE_ACTOR_HH
