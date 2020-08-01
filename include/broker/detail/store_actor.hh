#pragma once

#include <string>

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/response_handle.hpp>

#include "broker/defaults.hh"
#include "broker/detail/channel.hh"
#include "broker/endpoint.hh"
#include "broker/fwd.hh"
#include "broker/optional.hh"
#include "broker/topic.hh"

namespace broker::detail {

using local_request_key = std::pair<entity_id, request_id>;

} // namespace broker::detail

namespace std {

template <>
struct hash<broker::detail::local_request_key> {
  using value_type = broker::detail::local_request_key;

  size_t operator()(const value_type& x) const noexcept {
    // TODO: use caf::hash::fnv when switching to CAF 0.18.
    hash<value_type::first_type> f;
    hash<value_type::second_type> g;
    auto result = f(x.first);
    broker::detail::hash_combine(result, g(x.second));
    return result;
  }
};

} // namespace std

namespace broker::detail {

class store_actor_state {
public:
  // -- member types -----------------------------------------------------------

  /// Allows us to apply this state as a visitor to internal commands.
  using result_type = void;

  using channel_type = command_channel;

  using sequence_number_type = detail::sequence_number_type;

  using local_request_key = std::pair<entity_id, request_id>;

  // -- initialization ---------------------------------------------------------

  /// Initializes the state.
  /// @pre `ptr != nullptr`
  /// @pre `clock != nullptr`
  void init(caf::event_based_actor* self, endpoint_id this_endpoint,
            endpoint::clock* clock, std::string&& id, caf::actor&& core);

  template <class Backend, class Base>
  void init(channel_type::producer<Backend, Base>& out) {
    using caf::get_or;
    auto& cfg = self->config();
    out.heartbeat_interval(get_or(cfg, "broker.store.heartbeat-interval",
                                  defaults::store::heartbeat_interval));
    out.connection_timeout_factor(get_or(cfg, "broker.store.connection-timeout",
                                         defaults::store::connection_timeout));
  }

  template <class Backend, class RetryPolicy>
  void init(channel_type::consumer<Backend, RetryPolicy>& in) {
    using caf::get_or;
    auto& cfg = self->config();
    auto heartbeat_interval = get_or(cfg, "broker.store.heartbeat-interval",
                                     defaults::store::heartbeat_interval);
    auto connection_timeout = get_or(cfg, "broker.store.connection-timeout",
                                     defaults::store::connection_timeout);
    auto nack_timeout = get_or(cfg, "broker.store.nack-timeout",
                               defaults::store::nack_timeout);
    BROKER_DEBUG(BROKER_ARG(heartbeat_interval)
                 << BROKER_ARG(connection_timeout) << BROKER_ARG(nack_timeout));
    in.heartbeat_interval(heartbeat_interval);
    in.connection_timeout_factor(connection_timeout);
    in.nack_timeout(nack_timeout);
  }

  // -- event signaling --------------------------------------------------------

  /// Emits an `insert` event to topics::store_events subscribers.
  void emit_insert_event(const data& key, const data& value,
                         const optional<timespan>& expiry,
                         const entity_id& publisher);

  /// Convenience function for calling
  /// `emit_insert_event(msg.key, msg.value, msg.expiry)`.
  template <class Message>
  void emit_insert_event(const Message& msg) {
    emit_insert_event(msg.key, msg.value, msg.expiry, msg.publisher);
  }

  /// Emits a `update` event to topics::store_events subscribers.
  void emit_update_event(const data& key, const data& old_value,
                         const data& new_value,
                         const optional<timespan>& expiry,
                         const entity_id& publisher);

  /// Convenience function for calling
  /// `emit_update_event(msg.key, old_value, msg.value, msg.expiry,
  /// msg.publisher)`.
  template <class Message>
  void emit_update_event(const Message& msg, const data& old_value) {
    emit_update_event(msg.key, old_value, msg.value, msg.expiry, msg.publisher);
  }

  /// Emits an `erase` event to topics::store_events subscribers.
  void emit_erase_event(const data& key, const entity_id& publisher);

  /// Convenience function for calling
  /// `emit_erase_event(msg.key, msg.publisher)`.
  template <class Message>
  void emit_erase_event(const Message& msg) {
    emit_erase_event(msg.key, msg.publisher);
  }

  /// Emits an `expire` event to topics::store_events subscribers.
  void emit_expire_event(const data& key, const entity_id& publisher);

  /// Convenience function for calling
  /// `emit_expire_event(msg.key, msg.publisher)`.
  template <class Message>
  void emit_expire_event(const Message& msg) {
    emit_expire_event(msg.key, msg.publisher);
  }

  // -- callbacks for the behavior ---------------------------------------------

  void on_down_msg(const caf::actor_addr& source, const error& reason);

  // -- member variables -------------------------------------------------------

  /// Points to the actor owning this state.
  caf::event_based_actor* self = nullptr;

  /// Points to the endpoint's clock.
  endpoint::clock* clock = nullptr;

  /// Caches the configuration parameter `broker.store.tick-interval`.
  caf::timespan tick_interval;

  /// Stores the name, i.e., the prefix of the topic.
  std::string store_name;

  /// Stores the ID of this actor when communication to other store actors.
  entity_id id;

  /// Points the core actor of the endpoint this store belongs to.
  caf::actor core;

  /// Destination for emitted events.
  topic dst;

  /// Stores requests from local actors.
  std::unordered_map<local_request_key, caf::response_promise> local_requests;

  /// Stores promises to fulfill when reaching an idle state.
  std::vector<caf::response_promise> idle_callbacks;
};

} // namespace broker::detail
