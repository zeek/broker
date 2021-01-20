#pragma once

#include <cstdint>
#include <string>
#include <type_traits>

#include "broker/detail/enum_inspect.hh"

namespace broker {

/// Describes the possible states of a peer. A local peer begins in state
/// `initialized` and transitions directly to `peered`. A remote peer
/// begins in `initialized` and then through states `connecting`, `connected`,
/// and then `peered`.
enum class peer_status : unsigned {
  initialized,  ///< The peering process has been initiated.
  connecting,   ///< Connection establishment is in progress.
  connected,    ///< Connection has been established, peering pending.
  peered,       ///< Successfully peering.
  disconnected, ///< Connection to remote peer lost.
  reconnecting, ///< Reconnecting after a lost connection.
};

/// Convenience alias for the underlying integer type of @ref peer_status.
using peer_status_ut = std::underlying_type_t<peer_status>;

// -- conversion and inspection support ----------------------------------------

/// @relates peer_status
bool convert(peer_status src, peer_status_ut& dst) noexcept;

/// @relates peer_status
bool convert(peer_status src, std::string& dst);

/// @relates peer_status
bool convert(peer_status_ut src, peer_status& dst) noexcept;

/// @relates peer_status
bool convert(const std::string& src, peer_status& dst) noexcept;

/// @relates peer_status
std::string to_string(peer_status code);

/// @relates peer_status
template <class Inspector>
bool inspect(Inspector& f, peer_status& x) {
  return detail::enum_inspect(f, x);
}

} // namespace broker
