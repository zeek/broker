#ifndef BROKER_STATUS_HH
#define BROKER_STATUS_HH

#include <string>

#include "broker/detail/operators.hh"

#include "broker/endpoint_info.hh"

namespace broker {

/// The type of the different status information.
/// @relates status
enum status_info : uint8_t {
  unknown_status = 0,
  peer_added,         ///< Successfully peered
  peer_removed,       ///< Successfully unpeered
  peer_incompatible,  ///< Version incompatibility
  peer_invalid,       ///< Referenced peer does not exist
  peer_unavailable,   ///< Remote peer not listening
  peer_lost,          ///< Lost connection to peer
  peer_recovered,     ///< Re-gained connection to peer
};

/// @relates status_info
const char* to_string(status_info info);

/// A diagnostic message.
/// @relates status_info
struct status : detail::equality_comparable<status, status_info>,
                detail::equality_comparable<status_info, status> {
  /// Default-constructs an unknown status.
  status(status_info info = unknown_status);

  status(status_info info, endpoint_info local, endpoint_info remote,
         std::string message = {});

  status_info info;     ///< The ::status_info type.
  endpoint_info local;  ///< The local endpoint UID.
  endpoint_info remote; ///< The remote endpoint UID.
  std::string message;  ///< A descriptive context message.
};

/// @relates status
bool operator==(const status& s, status_info i);

/// @relates status
bool operator==(status_info i, const status& s);

/// @relates status
template <class Processor>
void serialize(Processor& proc, status& s) {
  proc & s.info;
  proc & s.local;
  proc & s.remote;
  proc & s.message;
}

} // namespace broker

#endif // BROKER_STATUS_HH
