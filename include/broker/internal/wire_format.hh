#pragma once

#include "broker/fwd.hh"

#include <caf/fwd.hpp>

namespace broker::internal {

/// Trait for caf::net::length_prefix_framing::run that translates between
/// native types and binary network representation.
struct wire_format {
  /// Serializes a @ref node_message to a sequence of bytes.
  bool convert(const node_message& msg, caf::byte_buffer& buf);

  /// Deserializes a @ref node_message from a sequence of bytes.
  bool convert(caf::const_byte_span bytes, node_message& msg);
};

} // namespace broker::internal
