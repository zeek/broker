#pragma once

#include <unordered_map>

#include <caf/actor.hpp>
#include <caf/behavior.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/telemetry/gauge.hpp>

#include "broker/data.hh"
#include "broker/detail/abstract_backend.hh"
#include "broker/endpoint.hh"
#include "broker/entity_id.hh"
#include "broker/fwd.hh"
#include "broker/internal/store_actor.hh"
#include "broker/internal_command.hh"
#include "broker/topic.hh"

namespace broker::internal {

class master_state : public store_actor_state {
public:
  // -- member types -----------------------------------------------------------

  /// Base type.
  using super = store_actor_state;

  /// Channel type for producing messages for a clone.
  using producer_type = channel_type::producer<master_state>;

  /// Channel type for consuming messages from a clone.
  using consumer_type = channel_type::consumer<master_state>;

  /// Owning smart pointer type to a backend.
  using backend_pointer = std::unique_ptr<detail::abstract_backend>;

  /// Bundles metrics for the master.
  struct metrics_t {
    metrics_t(caf::actor_system& sys, const std::string& name) noexcept;

    /// Keeps track of how many entries the store currently has.
    caf::telemetry::int_gauge* entries = nullptr;
  };

  template <class T>
  void broadcast(T&& cmd) {
    BROKER_TRACE(BROKER_ARG(cmd));
    // Suppress message if no one is listening.
    if (output.paths().empty())
      return;
    auto seq = output.next_seq();
    auto msg = make_command_message(clones_topic,
                                    internal_command{seq, id, entity_id::nil(),
                                                     std::forward<T>(cmd)});
    output.produce(std::move(msg));
  }

  // -- initialization ---------------------------------------------------------

  master_state(caf::event_based_actor* ptr, endpoint_id this_endpoint,
               std::string nm, backend_pointer bp, caf::actor parent,
               endpoint::clock* clock,
               caf::async::consumer_resource<command_message> in_res,
               caf::async::producer_resource<command_message> out_res);

  caf::behavior make_behavior();

  // -- callbacks for the behavior ---------------------------------------------

  void dispatch(const command_message& msg) override;

  table status_snapshot() const override;

  void tick();

  void set_expire_time(const data& key, const std::optional<timespan>& expiry);

  // -- callbacks for the consumer ---------------------------------------------

  void consume(consumer_type* src, command_message& cmd);

  void consume(put_command& cmd);

  void consume(put_unique_command& cmd);

  void consume(erase_command& cmd);

  void consume(add_command& cmd);

  void consume(subtract_command& cmd);

  void consume(clear_command& cmd);

  template <class T>
  void consume(T& cmd) {
    BROKER_ERROR("master got unexpected command:" << cmd);
  }

  error consume_nil(consumer_type* src);

  void close(consumer_type* src, const error&);

  void send(consumer_type*, channel_type::cumulative_ack);

  void send(consumer_type*, channel_type::nack);

  // -- callbacks for the producer ---------------------------------------------

  void send(producer_type*, const entity_id&, const channel_type::event&);

  void send(producer_type*, const entity_id&, channel_type::handshake);

  void send(producer_type*, const entity_id&, channel_type::retransmit_failed);

  void broadcast(producer_type*, channel_type::heartbeat);

  void broadcast(producer_type*, const channel_type::event&);

  void drop(producer_type*, const entity_id&, ec);

  void handshake_completed(producer_type*, const entity_id&);

  // -- properties -------------------------------------------------------------

  bool exists(const data& key);

  bool idle() const noexcept;

  // -- member variables -------------------------------------------------------

  /// Caches the topic for broadcasting to all clones.
  topic clones_topic;

  /// Manages the key-value store.
  backend_pointer backend;

  /// Manages outgoing commands.
  producer_type output;

  /// Maps senders to manager objects for incoming commands.
  std::unordered_map<entity_id, consumer_type> inputs;

  /// Maps senders to manager objects for incoming commands.
  std::unordered_map<entity_id, command_message> open_handshakes;

  /// Maps senders to manager objects for incoming commands.
  std::unordered_map<data, timestamp> expirations;

  /// Caches pointers to the metric instances.
  metrics_t metrics;

  /// Gives this actor a recognizable name in log files.
  static inline constexpr const char* name = "broker.master";
};

// -- master actor -------------------------------------------------------------

using master_actor_type = caf::stateful_actor<master_state>;

} // namespace broker::internal
