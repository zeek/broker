#include "broker/internal/message_provider.hh"

#include "broker/internal/wire_format.hh"
#include "broker/logger.hh"

namespace broker::internal {

const caf::chunk& message_provider::as_binary() {
  if (!binary_) {
    buffer_.clear();
    wire_format::v1::trait trait;
    if (!trait.convert(msg_, buffer_)) {
      log::core::error("message_provider",
                       "failed to convert message to chunk");
      return binary_;
    }
    binary_ = caf::chunk{buffer_};
  }
  return binary_;
}

} // namespace broker::internal
