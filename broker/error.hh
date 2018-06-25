#ifndef BROKER_ERROR_HH
#define BROKER_ERROR_HH

// ATTENTION
// ---------
// When updating this file, make sure to update doc/comm.rst as well because it
// copies parts of this file verbatim.
//
// Included lines: 23-51

#include <cstdint>
#include <utility>

#include <caf/atom.hpp>
#include <caf/error.hpp>
#include <caf/make_message.hpp>

namespace broker {

using caf::error;

/// Broker's status codes.
/// @relates status
enum class ec : uint8_t {
  /// The unspecified default error code.
  unspecified = 1,
  /// Version incompatibility.
  peer_incompatible,
  /// Referenced peer does not exist.
  peer_invalid,
  /// Remote peer not listening.
  peer_unavailable,
  /// An peering request timed out.
  peer_timeout,
  /// Master with given name already exist.
  master_exists,
  /// Master with given name does not exist.
  no_such_master,
  /// The given data store key does not exist.
  no_such_key,
  /// The store operation timed out.
  request_timeout,
  /// The operation expected a different type than provided
  type_clash,
  /// The data value cannot be used to carry out the desired operation.
  invalid_data,
  /// The storage backend failed to execute the operation.
  backend_failure,
  /// The clone store has not yet synchronized with its master, or it has
  /// been disconnected for too long.
  stale_data,
};

/// @relates ec
const char* to_string(ec code);

template <class... Ts>
error make_error(ec x, Ts&&... xs) {
  return {static_cast<uint8_t>(x), caf::atom("broker"),
          caf::make_message(std::forward<Ts>(xs)...)};
}

} // namespace broker

#endif // BROKER_ERROR_HH
