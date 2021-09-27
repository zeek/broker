#pragma once

#include <optional>
#include <unordered_map>
#include <vector>

#include <caf/actor.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/behavior.hpp>

#include "broker/data.hh"
#include "broker/detail/store_actor.hh"
#include "broker/endpoint.hh"
#include "broker/entity_id.hh"
#include "broker/internal_command.hh"
#include "broker/topic.hh"

namespace broker::detail {

class clone_state : public store_actor_state {
public:
  // -- member types -----------------------------------------------------------

  using super = store_actor_state;

  using consumer_type = channel_type::consumer<clone_state>;

  struct producer_base {
    /// Stores whether writes are currently disabled by the clone. This flag
    /// solves a race between the members `input` and `output_ptr` by disabling
    /// any output before the master completed the handshake with `input`.
    /// Without this stalling, a clone might "miss" its own writes. This becomes
    /// particularly problematic for `put_unique` operations if the master
    /// performs these operations before attaching the clone as a consumer.
    bool stalled = true;
  };

  using producer_type = channel_type::producer<clone_state, producer_base>;

  // -- initialization ---------------------------------------------------------

  clone_state(caf::event_based_actor* ptr, endpoint_id this_endpoint,
              std::string nm, caf::timespan master_timeout, caf::actor parent,
              endpoint::clock* ep_clock,
              caf::async::consumer_resource<command_message> in_res,
              caf::async::producer_resource<command_message> out_res);

  /// Sends `x` to the master.
  void forward(internal_command&& x);

  caf::behavior make_behavior();

  // -- callbacks for the behavior ---------------------------------------------

  void dispatch(const command_message& msg) override;

  void tick();

  // -- callbacks for the consumer ---------------------------------------------

  void consume(consumer_type*, command_message& msg);

  void consume(put_command& cmd);

  void consume(put_unique_result_command& cmd);

  void consume(erase_command& cmd);

  void consume(expire_command& cmd);

  void consume(clear_command& cmd);

  template <class T>
  void consume(T& cmd) {
    BROKER_ERROR("master got unexpected command:" << cmd);
  }

  error consume_nil(consumer_type* src);

  void close(consumer_type* src, error);

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

  /// Returns all keys of the store.
  data keys() const;

  /// Returns the writer instance, lazily creating it if necessary.
  producer_type& output();

  /// Sets the store content of the clone.
  void set_store(std::unordered_map<data, data> x);

  /// Returns whether the clone received a handshake from the master.
  bool has_master() const noexcept;

  bool idle() const noexcept;

  // -- member variables -------------------------------------------------------

  topic master_topic;

  std::unordered_map<data, data> store;

  consumer_type input;

  std::unique_ptr<producer_type> output_ptr;

  /// Stores the maximum amount of time that a master may take to perform the
  /// handshake or re-appear after a dropout.
  caf::timespan max_sync_interval;

  /// If set, marks when the clone stops trying to connect or re-connect to a
  /// master.
  std::optional<caf::timestamp> sync_timeout;

  static inline constexpr const char* name = "broker.clone";
};

// -- master actor -------------------------------------------------------------

using clone_actor_type = caf::stateful_actor<clone_state>;

caf::behavior clone_actor(clone_actor_type* self, endpoint_id this_endpoint,
                          caf::actor core, std::string id,
                          double resync_interval, double stale_interval,
                          double mutation_buffer_interval,
                          endpoint::clock* ep_clock);

} // namespace broker::detail
