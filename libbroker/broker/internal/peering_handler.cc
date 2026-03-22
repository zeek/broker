#include "broker/internal/peering_handler.hh"

#include "broker/detail/prefix_matcher.hh"
#include "broker/internal/core_actor.hh"
#include "broker/internal/message_provider.hh"
#include "broker/internal/pull_observer.hh"
#include "broker/internal/wire_format.hh"
#include "broker/logger.hh"
#include "broker/topic.hh"

namespace broker::internal {

peering_handler::~peering_handler() {
  auto n = static_cast<double>(queue.size());
  parent->metrics.buffered_messages->Decrement(n);
}

message_handler_type peering_handler::type() const noexcept {
  return message_handler_type::peering;
}

message_handler_offer_result
peering_handler::offer(message_provider& provider) {
  auto& msg = provider.get();
  // Filter out messages from other peers unless forwarding is enabled.
  auto&& src = get_sender(msg);
  if (parent->disable_forwarding && src.valid() && src != parent->id) {
    return message_handler_offer_result::skip;
  }
  // Filter out messages that aren't broadcasted (have no receiver) unless they
  // are directed to this peer explicitly.
  auto&& dst = get_receiver(msg);
  if (dst.valid() && dst != id) {
    return message_handler_offer_result::skip;
  }
  if (const auto& serialized = provider.as_binary()) {
    auto on_push = [this] {
      parent->metrics.buffered_messages->Increment();
      if (auto* lptr = logger()) {
        lptr->on_peer_buffer_push(id, 1);
      }
    };
    auto on_overflow = [this](const value_type& value) {
      // If the queue is full, we need to decide what to do with the message.
      if (auto* lptr = logger()) {
        lptr->on_peer_buffer_overflow(id, policy);
      }
      switch (policy) {
        default: // overflow_policy::disconnect
          log::core::info("offer",
                          "{} hit the buffer limit with policy disconnect",
                          pretty_name);
          out->abort(caf::make_error(caf::sec::backpressure_overflow));
          out = nullptr;
          return message_handler_offer_result::overflow_disconnect;
        case overflow_policy::drop_newest:
          queue.pop_back();
          queue.push_back(value);
          return message_handler_offer_result::ok;
        case overflow_policy::drop_oldest:
          queue.pop_front();
          queue.push_back(value);
          return message_handler_offer_result::ok;
      }
    };
    return do_offer(out, serialized, queue, on_push, on_overflow);
  }

  return message_handler_offer_result::skip;
}

void peering_handler::add_demand(size_t new_demand) {
  auto on_erase = [this](size_t n) {
    parent->metrics.buffered_messages->Decrement(static_cast<double>(n));
    if (auto* lptr = logger()) {
      lptr->on_peer_buffer_pull(id, n);
    }
  };
  do_add_demand(new_demand, queue, out, on_erase);
}

void peering_handler::dispose() {
  do_dispose(in, out);
}

bool peering_handler::input_closed() const noexcept {
  return !in;
}

bool peering_handler::output_closed() const noexcept {
  return !out;
}

bool peering_handler::pull() {
  // We pull chunks from the input buffer but the callee expects node messages.
  // Hence, we need to convert the chunks on the fly here.
  std::vector<value_type> chunks;
  chunks.reserve(128);
  auto result = do_pull(in, chunks);
  for (auto& item : chunks) {
    wire_format::v1::trait trait;
    node_message converted;
    if (!trait.convert(item.bytes(), converted)) {
      log::core::error("pull", "{} failed to convert chunk to node message",
                       pretty_name);
      in->dispose();
      in = nullptr;
      return false;
    }
    pull_buffer.emplace_back(std::move(converted));
  }
  return result;
}

node_message peering_handler::make_bye_message() {
  bye_token token;
  assign_bye_token(token);
  return make_ping_message(parent->id, id, token.data(), token.size());
}

void peering_handler::assign_bye_token(bye_token& buf) {
  const auto* prefix = "BYE";
  const auto* suffix = &bye_id;
  memcpy(buf.data(), prefix, 3);
  memcpy(buf.data() + 3, suffix, 8);
}

bool peering_handler::is_subscribed_to(const topic& what) const {
  detail::prefix_matcher f;
  return f(filter, what);
}

} // namespace broker::internal
