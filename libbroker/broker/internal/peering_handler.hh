#pragma once

#include "broker/endpoint_id.hh"
#include "broker/internal/message_handler.hh"
#include "broker/network_info.hh"
#include "broker/overflow_policy.hh"

#include <caf/async/spsc_buffer.hpp>
#include <caf/chunk.hpp>

namespace broker::internal {

class peering_handler : public message_handler {
public:
  // ASCII sequence 'BYE' followed by our 64-bit bye ID.
  static constexpr size_t bye_token_size = 11;

  using bye_token = std::array<std::byte, bye_token_size>;

  using value_type = caf::chunk;

  using buffer_producer_ptr = caf::async::spsc_buffer_producer_ptr<value_type>;

  using buffer_consumer_ptr = caf::async::spsc_buffer_consumer_ptr<value_type>;

  peering_handler(core_actor_state* parent, const endpoint_id& id,
                  size_t max_buffer_size, overflow_policy policy,
                  network_info addr, filter_type filter)
    : message_handler(parent, max_buffer_size),
      policy(policy),
      id(id),
      addr(std::move(addr)),
      filter(std::move(filter)) {
    // nop
  }

  ~peering_handler() override;

  message_handler_type type() const noexcept override;

  message_handler_offer_result offer(message_provider& msg) override;

  void add_demand(size_t new_demand) override;

  void dispose() override;

  bool input_closed() const noexcept override;

  bool output_closed() const noexcept override;

  message_handler_pull_result pull(std::vector<node_message>& buf) override;

  /// Creates a BYE message.
  node_message make_bye_message();

  /// Assigns a BYE token to a buffer.
  void assign_bye_token(bye_token& buf);

  // template <class Info, sc S>
  // node_message make_status_msg(Info&& ep, sc_constant<S> code,
  //                              const char* msg) {
  //   auto val = status::make(code, std::forward<Info>(ep), msg);
  //   auto content = get_as<data>(val);
  //   return make_data_message(parent->id, parent->id,
  //                            topic{std::string{topic::statuses_str}},
  //                            content);
  // }

  bool is_subscribed_to(const topic& what) const;

  /// The consumer for reading messages from the shared buffer.
  buffer_consumer_ptr in;

  /// The producer for writing messages to the shared buffer.
  buffer_producer_ptr out;

  /// A buffer for messages that are not yet sent to the peer.
  std::deque<value_type> queue;

  overflow_policy policy;

  /// The ID of this handler. Can be a peer ID or a randomly generated UUID.
  /// Only used for clients and peerings.
  endpoint_id id;

  /// Network address as reported from the transport (usually TCP).
  network_info addr;

  /// Stores the subscriptions of the remote peer.
  filter_type filter;

  /// A 64-bit token that we use as ping payload when unpeering. The ping is
  /// the last message we send. When receiving a pong message with that token,
  /// we know all messages arrived and can shut down the connection.
  uint64_t bye_id = 0;
};

using peering_handler_ptr = std::shared_ptr<peering_handler>;

} // namespace broker::internal
