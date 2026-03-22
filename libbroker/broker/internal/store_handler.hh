#pragma once

#include "broker/detail/assert.hh"
#include "broker/internal/message_handler.hh"
#include "broker/overflow_policy.hh"

#include <caf/actor.hpp>
#include <caf/async/spsc_buffer.hpp>

#include <string>

namespace broker::internal {

class store_handler : public message_handler {
public:
  using value_type = command_message;

  using id_type = std::string;

  using buffer_producer_ptr = caf::async::spsc_buffer_producer_ptr<value_type>;

  using buffer_consumer_ptr = caf::async::spsc_buffer_consumer_ptr<value_type>;

  store_handler(core_actor_state* parent, size_t max_buffer_size,
                std::string id, caf::actor hdl, message_handler_type subtype)
    : message_handler(parent, max_buffer_size),
      id(std::move(id)),
      hdl(std::move(hdl)),
      type_(subtype) {
    BROKER_ASSERT(subtype == message_handler_type::master
                  || subtype == message_handler_type::clone);
  }

  ~store_handler() override;

  message_handler_type type() const noexcept override;

  message_handler_offer_result offer(message_provider& msg) override;

  void add_demand(size_t new_demand) override;

  void dispose() override;

  bool input_closed() const noexcept override;

  bool output_closed() const noexcept override;

  message_handler_pull_result pull(std::vector<node_message>& buf) override;

  /// The consumer for reading messages from the shared buffer.
  buffer_consumer_ptr in;

  /// The producer for writing messages to the shared buffer.
  buffer_producer_ptr out;

  /// A buffer for messages that are not yet sent to the peer.
  std::deque<value_type> queue;

  /// The ID of this handler. Can be a peer ID or a randomly generated UUID.
  /// Only used for clients and peerings.
  id_type id;

  caf::actor hdl;

private:
  message_handler_type type_;
};

using store_handler_ptr = std::shared_ptr<store_handler>;

} // namespace broker::internal
