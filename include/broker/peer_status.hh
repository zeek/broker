#pragma once

namespace broker {

/// Describes the possible states of a peer. A local peer begins in state
/// `initialized` and transitions directly to `peered`. A remote peer
/// begins in `initialized` and then through states `connecting`, `connected`,
/// and then `peered`.
enum class peer_status {
  initialized,  ///< The peering process has been initiated.
  connecting,   ///< Connection establishment is in progress.
  connected,    ///< Connection has been established, peering pending.
  peered,       ///< Successfully peering.
  disconnected, ///< Connection to remote peer lost.
  reconnecting, ///< Reconnecting after a lost connection.
  unknown,      ///< No information available.
};

/// @relates peer_status
const char* to_string(peer_status);

/// @relates peer_status
template <class Inspector>
bool inspect(Inspector& f, peer_status& x) {
  auto get = [&] { return static_cast<int>(x); };
  auto set = [&](int val) {
    if (val >= 0 && val <= static_cast<int>(peer_status::reconnecting)) {
      x = static_cast<peer_status>(val);
      return true;
    } else {
      return false;
    }
  };
  return f.apply(get, set);
}

} // namespace broker
