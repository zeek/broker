#ifndef BROKER_ERROR_HH
#define BROKER_ERROR_HH

#include <caf/error.hpp>

#include "broker/message.hh"

namespace broker {

using caf::error;

/// Brokers's error codes.
enum class ec : uint8_t {
  /// The unspecified default error code.
  unspecified = 1,
  /// Version mismatch during peering.
  version_incompatible
};

/// @relates ec
const char* to_string(ec x);

/// @relates ec
template <class... Ts>
error make_error(ec x, Ts&&... xs) {
  return error{static_cast<uint8_t>(x), caf::atom("broker"),
               caf::make_message(std::forward<Ts>(xs)...)};
}

/// Simplifies generation of structured errors.
/// @relates ec
template <ec ErrorCode = ec::unspecified, class... Ts>
message make_error_message(Ts&&... xs) {
  return caf::make_message(make_error(ErrorCode, std::forward<Ts>(xs)...));
}

} // namespace broker

#endif // BROKER_ERROR_HH
