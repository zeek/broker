#pragma once

#include "broker/internal/connection_status.hh"
#include "broker/internal/message_handler_offer_result.hh"
#include "broker/internal/message_handler_pull_result.hh"
#include "broker/internal/message_handler_type.hh"
#include "broker/internal/pull_observer.hh"
#include "broker/message.hh"

#include <caf/callback.hpp>

#include <cstddef>
#include <deque>
#include <string>
#include <vector>

namespace broker::internal {

class message_provider;
class core_actor_state;

/// Bundles state for sending and receiving messages to/from a peer, hub,
/// store, or client.
class message_handler {
public:
  message_handler(core_actor_state* parent, size_t max_buffer_size)
    : parent(parent), max_buffer_size(max_buffer_size) {
    // nop
  }

  virtual ~message_handler();

  /// Called whenever dispatching a message that matches the handler's filter.
  [[nodiscard]] virtual message_handler_offer_result
  offer(message_provider& msg) = 0;

  /// Callback for a downstream component demanding more messages.
  virtual void add_demand(size_t demand) = 0;

  /// Tries to dispatch incoming messages.
  /// @param try_dispatch A callback that is called for each message in the
  ///                     buffer. Returning `false` stops the consume loop.
  template <class TryDispatch>
  message_handler_pull_result consume_while(TryDispatch&& try_dispatch) {
    // Helper function for dispatching messages from the buffer. Returns `false`
    // if `try_dispatch` returned `false`, i.e., if the caller stopped the loop.
    auto consume_from_buffer = [this, &try_dispatch] {
      auto pos = pull_buffer.begin();
      auto end = pull_buffer.end();
      while (pos != end) {
        if (!try_dispatch(*pos)) {
          // Delete what has been dispatched and leave the rest for later.
          pull_buffer.erase(pull_buffer.begin(), pos);
          return false;
        }
        ++pos;
      }
      pull_buffer.clear();
      return true;
    };
    // Consume messages from the buffer first.
    if (!consume_from_buffer()) {
      return message_handler_pull_result::ok;
    }
    // Try to get more messages.
    auto closed = !pull();
    // Consume messages from the buffer again.
    if (!consume_from_buffer() || !closed) {
      // Even if the producer closed the SPSC buffer, there is still data to
      // dispatch in the pull buffer.
      return message_handler_pull_result::ok;
    }
    // Tell the caller to not call this function again.
    return message_handler_pull_result::term;
  }

  /// Checks whether there is any data buffered in the input buffer for
  /// `consume_while`.
  bool has_input_data_buffered() const noexcept {
    return !pull_buffer.empty();
  }

  /// Disposes of the handler. Must be called after the handler has neither
  /// input nor output buffers and has been removed from its container.
  virtual void dispose() = 0;

  /// Checks whether the input buffer has been closed.
  virtual bool input_closed() const noexcept = 0;

  /// Checks whether the output buffer has been closed.
  virtual bool output_closed() const noexcept = 0;

  /// The type of the handler.
  virtual message_handler_type type() const noexcept = 0;

  /// The parent object.
  core_actor_state* parent;

  /// The name of this handler in log output.
  std::string pretty_name;

  /// Indicates whether we are unpeering from this node and the cause of the
  /// unpeering.
  connection_status status = connection_status::alive;

  /// A callback to be called when the handler is removed.
  caf::unique_callback_ptr<void()> on_dispose;

protected:
  template <class Input, class Output>
  void do_dispose(Input& in, Output& out) {
    if (in) {
      in->dispose();
    }
    if (out) {
      out->dispose();
    }
    if (on_dispose) {
      (*on_dispose)();
      on_dispose = nullptr;
    }
  }

  template <class ValueType, class Output, class OnErase>
  void do_add_demand(size_t new_demand, std::deque<ValueType>& queue,
                     Output& out, OnErase&& on_erase) {
    if (new_demand == 0) {
      return;
    }
    demand += new_demand;
    while (demand > 0 && !queue.empty()) {
      auto n = std::min(demand, queue.size());
      auto i = queue.begin();
      auto e = i + static_cast<std::ptrdiff_t>(n);
      std::vector<ValueType> items{i, e};
      on_erase(n);
      queue.erase(i, e);
      out->push(items);
      demand -= n;
    }
  }

  template <class Input, class ValueType>
  bool do_pull(Input& in, std::vector<ValueType>& buf) {
    if (!in) {
      return false;
    }
    pull_observer observer{buf};
    auto [again, pulled] = in->pull(100, observer);
    while (again && pulled == 100) {
      std::tie(again, pulled) = in->pull(100, observer);
    }
    if (!again) {
      in = nullptr;
      return false;
    }
    return true;
  }

  template <class Output, class ValueType, class OnPush, class OnOverload>
  message_handler_offer_result
  do_offer(Output& out, const ValueType& msg, std::deque<ValueType>& queue,
           OnPush&& on_push, OnOverload&& on_overflow) {
    // If output is closed, we can't send messages anymore
    if (!out) {
      return message_handler_offer_result::term;
    }
    // If we have demand, we can send the message immediately.
    if (demand > 0) {
      out->push(msg);
      --demand;
      return message_handler_offer_result::ok;
    }
    // As long as our queue is not full, we can store the message in our queue
    // until we have demand.
    if (queue.size() < max_buffer_size) {
      on_push();
      queue.push_back(std::move(msg));
      return message_handler_offer_result::ok;
    }
    // If the queue is full, we hit an overload condition and let the caller
    // decide what to do.
    return on_overflow(msg);
  }

  /// The maximum number of messages that can be buffered.
  size_t max_buffer_size;

  /// The number of messages that can be sent to the peer immediately.
  size_t demand = 0;

  /// A buffer for messages that are not yet dispatched.
  std::vector<node_message> pull_buffer;

private:
  /// Tries to pull more messages from the handler.
  /// @returns `true` if the producer is still alive, `false` if the input has
  ///          been closed.
  virtual bool pull() = 0;
};

using message_handler_ptr = std::shared_ptr<message_handler>;

} // namespace broker::internal
