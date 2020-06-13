#pragma once

#include <unordered_map>
#include <vector>

#include <caf/actor.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/behavior.hpp>

#include "broker/data.hh"
#include "broker/detail/store_actor.hh"
#include "broker/endpoint.hh"
#include "broker/internal_command.hh"
#include "broker/publisher_id.hh"
#include "broker/topic.hh"

namespace broker {
namespace detail {

class clone_state : public store_actor_state {
public:
  using super = store_actor_state;

  /// Initializes the state.
  void init(caf::event_based_actor* ptr, std::string&& nm,
            caf::actor&& parent, endpoint::clock* ep_clock);

  /// Sends `x` to the master.
  void forward(internal_command&& x);

  /// Wraps `x` into a `data` object and forwards it to the master.
  template <class T>
  void forward_from(T& x) {
    forward(make_internal_command<T>(std::move(x)));
  }

  void command(internal_command::variant_type& cmd);

  void command(internal_command& cmd);

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

  data keys() const;

  topic master_topic;

  caf::actor master;

  std::unordered_map<data, data> store;

  bool is_stale = true;

  double stale_time = -1.0;

  double unmutable_time = -1.0;

  std::vector<internal_command> mutation_buffer;

  std::vector<internal_command> pending_remote_updates;

  bool awaiting_snapshot = true;

  bool awaiting_snapshot_sync = true;

  static inline constexpr const char* name = "clone_actor";
};

caf::behavior clone_actor(caf::stateful_actor<clone_state>* self,
                          caf::actor core, std::string id,
                          double resync_interval, double stale_interval,
                          double mutation_buffer_interval,
                          endpoint::clock* ep_clock);

} // namespace detail
} // namespace broker
