#include "broker/internal/store_handler.hh"
#include "broker/internal/core_actor.hh"
#include "broker/internal/pull_observer.hh"

namespace broker::internal {

store_handler::~store_handler() {
  auto n = static_cast<double>(queue.size());
  parent->metrics.buffered_messages->Decrement(n);
}

message_handler_type store_handler::type() const noexcept {
  return type_;
}

message_handler_offer_result store_handler::offer(message_provider& msg) {
  if (auto cmd = msg.as_command()) {
    auto on_push = [this] { parent->metrics.buffered_messages->Increment(); };
    auto on_overflow = [this](const value_type&) {
      // Stores have a built-in mechanism for re-sending messages. Hence, we
      // can simply drop messages in case of overflow.
      return message_handler_offer_result::ok;
    };
    return do_offer(out, cmd, queue, on_push, on_overflow);
  }
  return message_handler_offer_result::skip;
}

void store_handler::add_demand(size_t new_demand) {
  auto on_erase = [this](size_t n) {
    parent->metrics.buffered_messages->Decrement(static_cast<double>(n));
  };
  do_add_demand(new_demand, queue, out, on_erase);
}

void store_handler::dispose() {
  do_dispose(in, out);
}

bool store_handler::input_closed() const noexcept {
  return !in;
}

bool store_handler::output_closed() const noexcept {
  return !out;
}

message_handler_pull_result
store_handler::pull(std::vector<node_message>& buf) {
  return do_pull(in, buf);
}

} // namespace broker::internal
