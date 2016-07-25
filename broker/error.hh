#ifndef BROKER_ERROR_HH
#define BROKER_ERROR_HH

#include <caf/error.hpp>
#include <caf/make_message.hpp>

namespace broker {

using caf::error;

/// Brokers's error codes.
enum class ec : uint8_t {
  /// The unspecified default error code.
  unspecified = 1,
  /// Version mismatch during peering.
  version_incompatible,
  /// Master with given name already exist.
  master_exists,
  /// Master with given name does not exist.
  no_such_master,
  /// The given data store key does not exist.
  no_such_key,
  /// The operation expected a different type than provided
  type_clash,
  /// The data value cannot be used to carry out the desired operation.
  invalid_data,
  /// The storage backend failed to execute the operation.
  backend_failure,
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
caf::message make_error_message(Ts&&... xs) {
  return caf::make_message(make_error(ErrorCode, std::forward<Ts>(xs)...));
}

} // namespace broker

#endif // BROKER_ERROR_HH
