#include "broker/internal/web_socket_client_handler.hh"

#include "broker/internal/core_actor.hh"
#include "broker/internal/message_provider.hh"
#include "broker/internal/pull_observer.hh"

namespace broker::internal {

web_socket_client_handler::~web_socket_client_handler() {
  auto n = static_cast<double>(queue.size());
  parent->metrics.buffered_messages->Decrement(n);
}

message_handler_type web_socket_client_handler::type() const noexcept {
  return message_handler_type::client;
}

message_handler_offer_result
web_socket_client_handler::offer(message_provider& msg) {
  if (auto dmsg = msg.as_data()) {
    auto on_push = [this] {
      parent->metrics.buffered_messages->Increment();
      if (auto* lptr = logger()) {
        lptr->on_client_buffer_push(id, 1);
      }
    };
    auto on_overflow = [this](const value_type& value) {
      if (auto* lptr = logger()) {
        lptr->on_client_buffer_overflow(id, policy);
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
    return do_offer(out, dmsg, queue, on_push, on_overflow);
  }
  return message_handler_offer_result::skip;
}

void web_socket_client_handler::add_demand(size_t new_demand) {
  auto on_erase = [this](size_t n) {
    parent->metrics.buffered_messages->Decrement(static_cast<double>(n));
    if (auto* lptr = logger()) {
      lptr->on_client_buffer_pull(id, n);
    }
  };
  do_add_demand(new_demand, queue, out, on_erase);
}

void web_socket_client_handler::dispose() {
  do_dispose(in, out);
}

bool web_socket_client_handler::input_closed() const noexcept {
  return !in;
}

bool web_socket_client_handler::output_closed() const noexcept {
  return !out;
}

message_handler_pull_result
web_socket_client_handler::pull(std::vector<node_message>& buf) {
  return do_pull(in, buf);
}

} // namespace broker::internal
