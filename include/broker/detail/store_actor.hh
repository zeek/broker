#pragma once

#include <string>

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>

#include "broker/endpoint.hh"

namespace broker::detail {

class store_actor_state {
public:
  /// Allows us to apply this state as a visitor to internal commands.
  using result_type = void;

  /// Initializes the state.
  /// @pre `ptr != nullptr`
  /// @pre `clock != nullptr`
  void init(caf::event_based_actor* self, endpoint::clock* clock,
            std::string&& id, caf::actor&& core);

  /// Emits an `add` event to topics::store_events subscribers.
  void emit_add_event(const data& key, const data& value,
                      const caf::optional<timespan>& expiry);

  /// Convenience function for calling
  /// `emit_add_event(msg.key, msg.value, msg.expiry)`.
  template <class Message>
  void emit_add_event(const Message& msg) {
    emit_add_event(msg.key, msg.value, msg.expiry);
  }

  /// Emits a `put` event to topics::store_events subscribers.
  void emit_put_event(const data& key, const data& value,
                      const caf::optional<timespan>& expiry);

  /// Convenience function for calling
  /// `emit_put_event(msg.key, msg.value, msg.expiry)`.
  template <class Message>
  void emit_put_event(const Message& msg) {
    emit_put_event(msg.key, msg.value, msg.expiry);
  }

  /// Emits an `erase` event to topics::store_events subscribers.
  void emit_erase_event(const data& key);

  /// Points to the actor owning this state.
  caf::event_based_actor* self = nullptr;

  /// Points to the endpoint's clock.
  endpoint::clock* clock = nullptr;

  /// Stores the ID of the store.
  std::string id;

  /// Points the core actor of the endpoint this store belongs to.
  caf::actor core;
};

} // namespace broker::detail
