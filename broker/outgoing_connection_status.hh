#ifndef BROKER_OUTGOING_CONNECTION_STATUS_HH
#define BROKER_OUTGOING_CONNECTION_STATUS_HH

#include <string>

#include "broker/peering.hh"

namespace broker {

/// A notification regarding an outgoing attempt to establish a peering
/// relationship between two endpoints.
struct outgoing_connection_status {
  /// The type of status notification.
  enum class tag : uint8_t {
    established,
    disconnected,
    incompatible,
  };

  /// The identity of a peering relationship between two endpoints.
  peering relation;

  /// A notification regarding the latest known status of the peering.
  tag status;

  /// When status is established, contains a name the peer chose for itself.
  std::string peer_name;
};

inline bool operator==(const outgoing_connection_status& lhs,
                       const outgoing_connection_status& rhs) {
  return lhs.status == rhs.status && lhs.relation == rhs.relation
         && lhs.peer_name == rhs.peer_name;
}

template <class Processor>
void serialize(Processor& proc, outgoing_connection_status& ocs,
               const unsigned) {
  proc& ocs.relation;
  proc& ocs.status;
  proc& ocs.peer_name;
}

} // namespace broker

#endif // BROKER_OUTGOING_CONNECTION_STATUS_HH
