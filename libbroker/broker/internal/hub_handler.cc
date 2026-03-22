#include "broker/internal/hub_handler.hh"

#include "broker/detail/assert.hh"
#include "broker/internal/core_actor.hh"

namespace broker::internal {

hub_handler::~hub_handler() {
  auto n = static_cast<double>(queue.size());
  parent->metrics.buffered_messages->Decrement(n);
}

message_handler_type hub_handler::type() const noexcept {
  return type_;
}

void hub_handler::type(message_handler_type subtype) {
  BROKER_ASSERT(subtype == message_handler_type::subscriber
                || subtype == message_handler_type::publisher);
  type_ = subtype;
}

message_handler_offer_result hub_handler::offer(message_provider& msg) {
  if (auto dmsg = msg.as_data()) {
    auto on_push = [this] { parent->metrics.buffered_messages->Increment(); };
    auto on_overflow = [this](const value_type&) {
      // Not supposed to happen. Drop the message and log an error.
      log::core::error("offer", "hub {} hit the buffer limit",
                       static_cast<uint64_t>(id));
      return message_handler_offer_result::ok;
    };
    return do_offer(out, dmsg, queue, on_push, on_overflow);
  }
  return message_handler_offer_result::skip;
}

void hub_handler::add_demand(size_t new_demand) {
  auto on_erase = [this](size_t n) {
    parent->metrics.buffered_messages->Decrement(static_cast<double>(n));
  };
  do_add_demand(new_demand, queue, out, on_erase);
}

void hub_handler::dispose() {
  do_dispose(in, out);
}

bool hub_handler::input_closed() const noexcept {
  return !in;
}

bool hub_handler::output_closed() const noexcept {
  return !out;
}

bool hub_handler::pull() {
  return do_pull(in, pull_buffer);
}

} // namespace broker::internal
