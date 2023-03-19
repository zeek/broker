#pragma once

#include <algorithm>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <caf/actor.hpp>
#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/async/spsc_buffer.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/flow/item_publisher.hpp>
#include <caf/flow/observable.hpp>
#include <caf/hash/fnv.hpp>
#include <caf/response_handle.hpp>

#include "broker/data.hh"
#include "broker/defaults.hh"
#include "broker/detail/store_state.hh"
#include "broker/endpoint.hh"
#include "broker/fwd.hh"
#include "broker/internal/channel.hh"
#include "broker/internal/type_id.hh"
#include "broker/topic.hh"

namespace broker::internal {

using local_request_key = std::pair<entity_id, request_id>;

} // namespace broker::internal

namespace std {

template <>
struct hash<broker::internal::local_request_key> {
  size_t
  operator()(const broker::internal::local_request_key& x) const noexcept {
    return caf::hash::fnv<size_t>::compute(x.first, x.second);
  }
};

} // namespace std

namespace broker::internal {

class store_actor_state {
public:
  // -- member types -----------------------------------------------------------

  /// Allows us to apply this state as a visitor to internal commands.
  using result_type = void;

  using channel_type = channel<entity_id, command_message>;

  using local_request_key = std::pair<entity_id, request_id>;

  // -- constructors, destructors, and assignment operators --------------------

  store_actor_state(caf::event_based_actor* self);

  virtual ~store_actor_state();

  // -- initialization ---------------------------------------------------------

  /// Initializes the state.
  /// @pre `ptr != nullptr`
  /// @pre `clock != nullptr`
  void init(endpoint_id this_endpoint, endpoint::clock* clock, std::string&& id,
            caf::actor&& core,
            caf::async::consumer_resource<command_message> in_res,
            caf::async::producer_resource<command_message> out_res);

  template <class Backend, class Base>
  void init(channel_type::producer<Backend, Base>& out) {
    using caf::get_or;
    auto& cfg = self->config();
    out.heartbeat_interval(get_or(cfg, "broker.store.heartbeat-interval",
                                  defaults::store::heartbeat_interval));
    out.connection_timeout_factor(get_or(cfg, "broker.store.connection-timeout",
                                         defaults::store::connection_timeout));
    out.metrics().init(self->system(), store_name);
  }

  template <class Backend>
  void init(channel_type::consumer<Backend>& in) {
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
    in.metrics().init(self->system(), store_name);
  }

  template <class... Fs>
  caf::behavior make_behavior(Fs... fs) {
    BROKER_TRACE("");
    return {
      std::move(fs)...,
      [this](atom::increment, detail::shared_store_state_ptr ptr) {
        attached_states.emplace(std::move(ptr), size_t{0}).first->second += 1;
      },
      [this](atom::decrement, const detail::shared_store_state_ptr& ptr) {
        auto& xs = attached_states;
        if (auto i = xs.find(ptr); i != xs.end())
          if (--(i->second) == 0)
            xs.erase(i);
      },
      [this](atom::get, atom::status) { return status_snapshot(); },
    };
  }

  // -- event signaling --------------------------------------------------------

  /// Emits an `insert` event to topics::store_events subscribers.
  void emit_insert_event(const data& key, const data& value,
                         const std::optional<timespan>& expiry,
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
                         const std::optional<timespan>& expiry,
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

  virtual void dispatch(const command_message& msg) = 0;

  /// Creates a snapshot that summarizes the current status of the store actor.
  virtual table status_snapshot() const = 0;

  void on_down_msg(const caf::actor_addr& source, const caf::error& reason);

  // -- convenience functions --------------------------------------------------

  /// Sends a delayed message by using the endpoint's clock.
  void send_later(const caf::actor& hdl, timespan delay, caf::message msg);

  template <class Derived>
  static table get_stats(const channel_type::consumer<Derived>& in) {
    using namespace std::literals;
    table result;
    result.emplace("next-seq"s, in.next_seq());
    result.emplace("last-seq"s, in.last_seq());
    result.emplace("num-ticks"s, in.num_ticks());
    if (auto out_of_order_updates = in.metrics().out_of_order_updates)
      result.emplace("buffered"s, out_of_order_updates->value());
    return result;
  }

  template <class Derived>
  static table get_stats(const channel_type::producer<Derived>& out) {
    using namespace std::literals;
    table result;
    result.emplace("seq"s, out.seq());
    result.emplace("tick-time"s, out.tick_time().value);
    result.emplace("buffered"s, out.buf().size());
    vector paths;
    for (const auto& path : out.paths()) {
      table entry;
      entry.emplace("id"s, to_string(path.hdl.endpoint));
      entry.emplace("offset"s, path.offset);
      entry.emplace("acked"s, path.acked);
      entry.emplace("last-seen"s, path.last_seen.value);
      paths.emplace_back(std::move(entry));
    }
    result.emplace("paths"s, std::move(paths));
    return result;
  }

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

  /// Strong pointers for all locally attached store objects. The stores
  /// themselves only keep weak pointers to their state in order to couple the
  /// validity of their state to the lifetime of their (frontend) actor. The
  /// `size_t` value reflects the number of `store` objects that currently have
  /// access to the stored state.
  /// @note the state keeps an actor handle to this actor, but CAF breaks this
  ///       cycle automatically by destroying this vector when the actor
  ///       terminates.
  std::unordered_map<detail::shared_store_state_ptr, size_t> attached_states;

  caf::flow::item_publisher<command_message> out;
};

} // namespace broker::internal
