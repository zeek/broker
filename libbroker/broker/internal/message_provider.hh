#pragma once

#include "broker/message.hh"

#include <caf/chunk.hpp>

namespace broker::internal {

/// Wraps access to a node message and also allows converting it to a chunk.
/// This conversion is done lazily, i.e., only when the message is actually
/// needed. Furthermore, if multiple handlers require the conversion, it is
/// done only once and the result is cached.
class message_provider {
public:
  void set(node_message what) {
    msg_ = std::move(what);
    binary_ = caf::chunk{};
  }

  const node_message& get() const {
    return msg_;
  }

  const caf::chunk& as_binary();

  data_message as_data() {
    if (msg_->type() == envelope_type::data) {
      return msg_->as_data();
    }
    return nullptr;
  }

  command_message as_command() {
    if (msg_->type() == envelope_type::command) {
      return msg_->as_command();
    }
    return nullptr;
  }

private:
  /// The current message.
  node_message msg_;

  /// Caches the serialized message.
  caf::chunk binary_;

  /// Buffer for serializing the message to a chunk. Re-used for multiple
  /// conversions to avoid allocating a new buffer for each conversion.
  caf::byte_buffer buffer_;
};

} // namespace broker::internal
