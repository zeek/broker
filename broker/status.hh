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

/// Diagnostic status information.
struct status : detail::equality_comparable<status, status_info>,
                detail::equality_comparable<status_info, status> {
  status(status_info info = unknown_status);

  status_info info;       ///< The ::status_info type.
  endpoint_info endpoint; ///< The affected endpoint.
  std::string message;    ///< A human-readable description of the event.
};

/// @relates status
bool operator==(const status& s, status_info i);

/// @relates status
bool operator==(status_info i, const status& s);

/// @relates status
template <class Processor>
void serialize(Processor& proc, status& s) {
  proc & s.info;
  proc & s.endpoint;
  proc & s.message;
}

} // namespace broker

#endif // BROKER_STATUS_HH
