#pragma once

#include "broker/fwd.hh"

#include <caf/fwd.hpp>

#include <cstddef>
#include <vector>

namespace broker::internal {

/// Bundles utility functions for JSON handling.
class json {
public:
  /// Converts @p msg to a JSON object.
  static void apply(const data_message& msg, caf::json_writer& out);

  /// Converts a JSON object that represents a @ref data_message to a binary
  /// representation.
  static error data_message_to_binary(const caf::json_object& obj,
                                      std::vector<std::byte>& buf);
};

} // namespace broker::internal
