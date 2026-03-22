#pragma once

#include "broker/internal/message_handler.hh"
#include "broker/message.hh"
#include "broker/overflow_policy.hh"

#include <caf/async/spsc_buffer.hpp>

namespace broker::internal {

class web_socket_client_handler : public message_handler {
public:
  using value_type = data_message;

  using buffer_producer_ptr = caf::async::spsc_buffer_producer_ptr<value_type>;

  using buffer_consumer_ptr = caf::async::spsc_buffer_consumer_ptr<value_type>;

  web_socket_client_handler(core_actor_state* parent, size_t max_buffer_size,
                            overflow_policy policy, const endpoint_id& id)
    : message_handler(parent, max_buffer_size), policy(policy), id(id) {
    // nop
  }

  ~web_socket_client_handler() override;

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

  overflow_policy policy;

  /// The ID for this WebSocket connection.
  endpoint_id id;
};

using web_socket_client_handler_ptr =
  std::shared_ptr<web_socket_client_handler>;

} // namespace broker::internal
